use actix_cors::Cors;
use actix_web::App;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::HttpServer;
use actix_web::web;
use actix_web::web::Data;
use actix_web::web::Payload;
use actix_ws::AggregatedMessage;
use actix_ws::AggregatedMessageStream;
use actix_ws::Session;
use actix_ws::handle;
use anyhow::Result;
use anyhow::bail;
use database::db;
use futures_util::StreamExt;
use infrastructure::jwt::Jwt;
use kernel::server::WSMessage;
use kernel::server::WSServer;
use kernel::server_methods::ServerMethods;
use kernel::types::HOST;
use kernel::types::PORT;
use utility::types::LogError;

type ServerMethodsType = ServerMethods<Jwt, db::S>;

pub async fn main() {
    println!("started server");
    let actions = Data::new(ServerMethodsType::new().await);

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .app_data(actions.clone())
            .route("/ws", web::get().to(ws_handler))
    })
    // .bind_rustls_0_23((HOST, PORT), get_tls_config())
    .bind((HOST, PORT))
    .unwrap()
    .run()
    .await
    .unwrap()
}

async fn ws_handler(req: HttpRequest, stream: Payload) -> HttpResponse {
    let (response, session, stream) = match handle(&req, stream) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Error upgrading to WebSocket: {}", e);
            return HttpResponse::BadRequest().finish();
        }
    };

    let stream = stream.aggregate_continuations().max_continuation_size(2_usize.pow(16));

    let session = WsType::new(session, stream);
    let state = req.app_data::<Data<ServerMethodsType>>().unwrap();
    state.clone().into_inner().server_actor::<WsType>(session);

    response
}

struct WsType {
    session: Session,
    stream:  AggregatedMessageStream,
}

impl WsType {
    fn new(session: Session, stream: AggregatedMessageStream) -> Self {
        Self {
            session,
            stream,
        }
    }
}

impl WSServer for WsType {
    async fn send_bin(&mut self, bin: Vec<u8>) -> Result<()> {
        self.session.binary(bin).await.log()?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<WSMessage> {
        match self.stream.next().await {
            Some(msg) => {
                match msg.log()? {
                    AggregatedMessage::Binary(data) => Ok(WSMessage::Binary(data.to_vec())),
                    AggregatedMessage::Text(_) => bail!("we dont use text"),
                    AggregatedMessage::Ping(_) => {
                        todo!()
                    }
                    AggregatedMessage::Pong(_) => {
                        todo!()
                    }
                    AggregatedMessage::Close(_) => Ok(WSMessage::Close),
                }
            }
            None => bail!("WebSocket connection closed"),
        }
    }

    async fn close(self) -> Result<()> {
        self.session.clone().close(None).await.log()?;
        Ok(())
    }
}

#[allow(dead_code)]
fn get_tls_config() -> rustls::ServerConfig {
    rustls::crypto::aws_lc_rs::default_provider().install_default().unwrap();

    const CERT_PEM: &[u8] = include_bytes!("../../../privet/cert.pem");
    const KEY_PEM: &[u8] = include_bytes!("../../../privet/key.pem");

    use std::io::BufReader;

    let mut certs_file = BufReader::new(CERT_PEM);
    let mut key_file = BufReader::new(KEY_PEM);

    // load TLS certs and key
    // to create a self-signed temporary cert for testing:
    // `openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 -subj '/CN=localhost'`
    let tls_certs = rustls_pemfile::certs(&mut certs_file).collect::<Result<_, _>>().unwrap();
    let tls_key = rustls_pemfile::pkcs8_private_keys(&mut key_file).next().unwrap().unwrap();

    // set up TLS config options
    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(tls_certs, rustls::pki_types::PrivateKeyDer::Pkcs8(tls_key))
        .unwrap()
}
