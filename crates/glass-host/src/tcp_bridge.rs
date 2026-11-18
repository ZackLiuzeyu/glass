//! A WebSocket transport for tailnets whose host runs Tailscale in userspace mode.
//! Tailscale Serve can forward HTTPS and WebSockets to loopback, but cannot forward
//! Glass's UDP WebTransport listener on macOS. The bridge keeps Glass's existing
//! authentication and session protocol by connecting to that listener locally.

use std::collections::HashMap;
use std::net::IpAddr;

use anyhow::{Context, Result, bail};
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::routing::get;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{debug, info};

const DATAGRAM: u8 = 0;
const OPEN_BI: u8 = 1;
const OPEN_UNI: u8 = 2;
const DATA: u8 = 3;
const END: u8 = 4;
const INCOMING_UNI: u8 = 5;
const CHUNK: usize = 16 * 1024;
const QUEUE: usize = 512;

pub fn routes(wt_port: u16, dial: IpAddr) -> Router {
    Router::new().route(
        "/tcp-bridge",
        get(move |upgrade: WebSocketUpgrade| async move {
            upgrade.on_upgrade(move |socket| async move {
                if let Err(error) = bridge(socket, wt_port, dial).await {
                    info!("TCP bridge ended: {error:#}");
                }
            })
        }),
    )
}

fn packet(kind: u8, id: u32, payload: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(kind);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(payload);
    Bytes::from(out)
}

fn parse(bytes: &[u8]) -> Result<(u8, u32, &[u8])> {
    if bytes.len() < 5 {
        bail!("short TCP bridge packet")
    }
    let id = u32::from_be_bytes(bytes[1..5].try_into()?);
    Ok((bytes[0], id, &bytes[5..]))
}

async fn stream_to_ws(mut recv: wtransport::RecvStream, id: u32, output: mpsc::Sender<Bytes>) {
    let mut buf = vec![0; CHUNK];
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                if output.send(packet(DATA, id, &buf[..n])).await.is_err() {
                    return;
                }
            }
            Ok(None) | Err(_) => {
                let _ = output.send(packet(END, id, &[])).await;
                return;
            }
        }
    }
}

async fn bridge(socket: WebSocket, wt_port: u16, dial: IpAddr) -> Result<()> {
    let config = wtransport::ClientConfig::builder()
        .with_bind_default()
        .with_no_cert_validation()
        .build();
    let endpoint = wtransport::Endpoint::client(config)?;
    let (bridge_id, _viewer, _registration) = crate::remote::register_bridge();
    // /rtc marks this as a bridged session, so a loopback connection is never
    // mistaken for an already trusted tailnet viewer. Glass still checks its token.
    let url = format!(
        "https://{}:{wt_port}/rtc?peer=127.0.0.1&b={bridge_id}&k={}",
        crate::listen::host(dial),
        crate::remote::bridge_secret()
    );
    let conn = endpoint
        .connect(url)
        .await
        .context("local WebTransport session")?;
    crate::server::remote_viewer_joined();
    let (mut ws_out, mut ws_in) = socket.split();
    let (output, mut outgoing) = mpsc::channel::<Bytes>(QUEUE);
    let writer = tokio::spawn(async move {
        while let Some(bytes) = outgoing.recv().await {
            if ws_out.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }
    });

    // Video and audio datagrams are disposable under pressure; control streams
    // below use await, so they are never dropped by this queue.
    let out = output.clone();
    let dgram_conn = conn.clone();
    let datagrams = tokio::spawn(async move {
        while let Ok(datagram) = dgram_conn.receive_datagram().await {
            let _ = out.try_send(packet(DATAGRAM, 0, &datagram.payload()));
        }
    });
    let out = output.clone();
    let incoming_conn = conn.clone();
    let incoming = tokio::spawn(async move {
        let mut next = 0x8000_0000_u32;
        while let Ok(recv) = incoming_conn.accept_uni().await {
            let id = next;
            next = next.wrapping_add(1);
            if out.send(packet(INCOMING_UNI, id, &[])).await.is_err() {
                break;
            }
            tokio::spawn(stream_to_ws(recv, id, out.clone()));
        }
    });

    let mut sends: HashMap<u32, wtransport::SendStream> = HashMap::new();
    let result = async {
        while let Some(message) = ws_in.next().await {
            let Message::Binary(bytes) = message? else {
                continue;
            };
            let (kind, id, payload) = parse(&bytes)?;
            match kind {
                DATAGRAM => {
                    conn.send_datagram(payload)?;
                }
                OPEN_BI => {
                    let (send, recv) = conn.open_bi().await?.await?;
                    if sends.insert(id, send).is_some() {
                        bail!("duplicate bridge stream")
                    }
                    tokio::spawn(stream_to_ws(recv, id, output.clone()));
                }
                OPEN_UNI => {
                    let send = conn.open_uni().await?.await?;
                    if sends.insert(id, send).is_some() {
                        bail!("duplicate bridge stream")
                    }
                }
                DATA => {
                    let Some(send) = sends.get_mut(&id) else {
                        bail!("unknown bridge stream")
                    };
                    send.write_all(payload).await?;
                }
                END => {
                    if let Some(mut send) = sends.remove(&id) {
                        send.finish().await?;
                    }
                }
                _ => bail!("unknown TCP bridge packet kind"),
            }
        }
        Ok(())
    }
    .await;
    debug!("TCP bridge disconnecting");
    conn.close(0u32.into(), b"websocket closed");
    datagrams.abort();
    incoming.abort();
    writer.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_round_trip() {
        let bytes = packet(DATA, 42, b"hello");
        assert_eq!(parse(&bytes).unwrap(), (DATA, 42, b"hello".as_slice()));
        assert!(parse(&bytes[..4]).is_err());
    }
}
