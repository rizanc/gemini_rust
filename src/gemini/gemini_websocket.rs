use base64::engine::general_purpose;
use base64::Engine;
use hmac::{Hmac, Mac};
use http::Request;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha384;
use std::net::TcpStream;

use tungstenite::handshake::client::Response;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Error, Result, WebSocket};
use url::Url;

extern crate chrono;

#[derive(Debug, Serialize, Deserialize)]
pub struct CandleUpdate {
    pub changes: Vec<Vec<f64>>,
    pub symbol: String,
    #[serde(rename = "type")]
    message_type: String, // 'type' is a reserved keyword in Rust
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    timestamp: Option<i64>,
    timestampms: Option<i64>,
    #[serde(rename = "type")]
    message_type: String,
    sequence: Option<i32>,
}

pub fn create_v1_marketdata_ws(
    symbol: &str,
) -> std::result::Result<(WebSocket<MaybeTlsStream<TcpStream>>, Response), Box<dyn std::error::Error>>
{
    let api_url = format!("wss://api.gemini.com/v1/marketdata/{}?trades=false&bids=true&offers=false&heartbeat=true&top_of_book=true",symbol);

    match connect(Url::parse(&api_url)?) {
        Ok((socket, response)) => {
            Result::Ok((socket, response))
        }
        Err(err) => {

            Err(Box::new(err))
        }
    }
}

pub fn create_v2_marketdata_ws() -> Result<(WebSocket<MaybeTlsStream<TcpStream>>, Response)> {
    let api_url = "wss://api.gemini.com/v2/marketdata";

    match connect(Url::parse(api_url).unwrap()) {
        Ok((socket, response)) => {
            debug!("Connected to the server {}", api_url);
            Result::Ok((socket, response))
        }
        Err(error) => {
            if let Error::Http(ref f) = &error {
                debug!("{:?}", f.headers());
            }
            debug!("{:?}", &error);
            Err(error)
        }
    }
}

pub async fn create_order_events_ws(
    payload: &Value,
) -> Result<(
    WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>,
    Response,
)> {
    let key = std::env::var("gemini_key").unwrap();
    let secret = std::env::var("gemini_secret").unwrap();

    let payload_str = payload.to_string();
    let b64_payload = general_purpose::STANDARD.encode(&payload_str);

    // Create a SHA384 HMAC using the secret
    let mut mac =
        Hmac::<Sha384>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");

    mac.update(b64_payload.as_bytes());

    let signature = mac.finalize().into_bytes();

    // Convert HMAC result to a hexadecimal string
    let signature_hex = hex::encode(signature);

    let api_url = "wss://api.gemini.com/v1/order/events";

    let request = Request::builder()
        .uri(api_url)
        .header("X-GEMINI-PAYLOAD", b64_payload)
        .header("X-GEMINI-APIKEY", key.clone())
        .header("X-GEMINI-SIGNATURE", signature_hex)
        .header("Host", "me.home")
        .header("Origin", "me.origine")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .body(())
        .expect("Failed to build request.");

    let (socket, response) = connect(request)?;
    Ok((socket, response))
}
