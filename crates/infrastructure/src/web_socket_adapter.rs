use anyhow::Result;

pub trait WSClient: Sized {
    fn connect(url: &str) -> impl Future<Output = Result<Self>>;
    fn send_bin(&mut self, data: &[u8]) -> impl Future<Output = Result<()>>;
    fn receive_bin(&mut self) -> impl Future<Output = Result<Vec<u8>>>;
}

pub type Ws = target::S;

#[cfg(not(target_arch = "wasm32"))]
pub mod target {
    use super::WSClient;
    use anyhow::Result;
    use anyhow::bail;
    use futures::SinkExt;
    use futures::StreamExt;
    use futures::stream::SplitSink;
    use futures::stream::SplitStream;
    use tokio::net::TcpStream;
    use tokio_tungstenite::MaybeTlsStream;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    pub struct S {
        write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        read:  SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    }

    impl WSClient for S {
        async fn connect(url: &str) -> Result<Self> {
            let (ws_stream, _) = connect_async(url).await?;
            let (write, read) = ws_stream.split();

            Ok(Self {
                write,
                read,
            })
        }

        async fn send_bin(&mut self, data: &[u8]) -> Result<()> {
            self.write.send(Message::Binary(data.to_vec().into())).await?;

            Ok(())
        }

        async fn receive_bin(&mut self) -> Result<Vec<u8>> {
            match self.read.next().await {
                Some(Ok(message)) => {
                    match message {
                        Message::Text(_) => bail!("it's text"),
                        Message::Binary(bytes) => Ok(bytes.to_vec()),
                        Message::Close(_) => bail!("connection closed"),
                        _ => bail!("other message type"),
                    }
                }
                Some(Err(e)) => Err(e.into()),
                None => bail!("connection closed"),
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub mod target {
    use anyhow::Result;
    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use futures_util::stream::SplitSink;
    use futures_util::stream::SplitStream;
    use gloo_net::websocket::Message;
    use gloo_net::websocket::futures::WebSocket;
    use my_core::client::network_actor::WSClient;
    use std::sync::Mutex;

    pub struct S {
        write: SplitSink<WebSocket, Message>,
        read:  SplitStream<WebSocket>,
    }

    impl WSClient for S {
        async fn connect(url: &str) -> Result<Self> {
            let ws = WebSocket::open(url)?;
            let (mut write, read) = ws.split();
            write.send(Message::Bytes(Vec::new())).await?;

            Ok(Self {
                write,
                read,
            })
        }

        async fn send_bin(&mut self, data: &[u8]) -> Result<()> {
            self.write.send(Message::Bytes(data.clone().into())).await?;

            Ok(())
        }

        async fn receive_bin(&mut self) -> Result<Vec<u8>> {
            match self.read.next().await {
                Some(Ok(message)) => {
                    match message {
                        Message::Text(_) => bail!("it's text"),
                        Message::Bytes(bytes) => Ok(bytes.to_vec()),
                    }
                }
                Some(Err(e)) => Err(e.into()),
                None => bail!("connection closed"),
            }
        }
    }
}
