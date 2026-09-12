use anyhow::Error;
use anyhow::Result;
use anyhow::bail;
use infrastructure::runtime::Either;
use infrastructure::runtime::Rt;
use infrastructure::runtime::Runtime;
use infrastructure::web_socket_adapter::WSClient;
use infrastructure::web_socket_adapter::Ws;
use std::time::Duration;

pub trait Network {
    const SLEEP_DURATION: Duration = Duration::from_millis(100);
    fn network_state(&mut self, is_online: bool) -> impl Future<Output = ()>;
    fn network_sender(&mut self, data: Vec<u8>) -> impl Future<Output = ()>;
    fn network_reciever(&mut self) -> impl Future<Output = Vec<u8>>;
    fn send_error(&mut self, error: Error) -> impl Future<Output = ()>;
}

async fn network_radar(ws: Option<&mut Ws>) -> Result<Vec<u8>> {
    match ws {
        Some(ws) => ws.receive_bin().await,
        None => bail!("error"),
    }
}

async fn connect<Nw: Network>(network_utils: &mut Nw, url: &str, ws: &mut Option<Ws>) {
    network_utils.network_state(false).await;

    if let Ok(ok) = Ws::connect(url).await {
        *ws = Some(ok);
        network_utils.network_state(true).await;
        return;
    }
    Rt::sleep(Nw::SLEEP_DURATION).await;
}

pub fn network_actor<Nw: Network + 'static>(mut network_utils: Nw, url: String) {
    Rt::spawn_local(async move {
        let mut ws: Option<Ws> = None;

        loop {
            match Rt::select(network_utils.network_reciever(), network_radar(ws.as_mut())).await {
                Either::One(data) => {
                    match &mut ws {
                        Some(ws1) => {
                            let result = ws1.send_bin(&data).await;
                            if result.is_err() {
                                connect::<Nw>(&mut network_utils, &url, &mut ws).await;
                            }
                        }
                        None => Rt::sleep(Nw::SLEEP_DURATION).await,
                    }
                }

                Either::Two(from_network) => {
                    match from_network {
                        Ok(data) => {
                            network_utils.network_sender(data).await;
                        }
                        Err(error) => {
                            network_utils.send_error(error).await;
                            connect::<Nw>(&mut network_utils, &url, &mut ws).await;
                        }
                    }
                }
            }
        }
    });
}
