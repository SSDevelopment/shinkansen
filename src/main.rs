use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use dotenv::dotenv;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio;

#[derive(Clone)]
struct AppState {
    producer: FutureProducer,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let cert = fs::read("certs/service.cert")?;
    let key = fs::read("certs/service.key")?;
    let ca_cert = fs::read("certs/ca.pem")?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", env::var("BOOTSTRAP_SERVER")?)
        .set("security.protocol", "ssl")
        .set("ssl.certificate.pem", String::from_utf8(cert)?)
        .set("ssl.key.pem", String::from_utf8(key)?)
        .set("ssl.ca.pem", String::from_utf8(ca_cert)?)
        .set("message.timeout.ms", env::var("MESSAGE_TIMEOUT")?)
        .create()?;

    let state = Arc::new(AppState { producer });

    let app = Router::new()
        .route("/forward-message/:topic", post(forward_message))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", env::var("INBOUND_PORT")?))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}

async fn forward_message(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    state
        .producer
        .send(
            FutureRecord::to(&topic).payload(&body[..]).key(""),
            Duration::from_secs(0),
        )
        .await
        .unwrap();

    StatusCode::OK
}
