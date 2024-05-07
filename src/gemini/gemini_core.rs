use std::{error::Error, io::ErrorKind};

use hex;
use hmac::{Hmac, Mac};
use reqwest;
use reqwest::Response;
use serde_json::Value;
use sha2::Sha384;
extern crate chrono;
use base64::{engine::general_purpose, Engine as _};

use anyhow::{Result,anyhow};

pub async fn post(url: &str, payload: &Value) -> Result<Response> {
    let key = std::env::var("gemini_key")?;
    let secret = std::env::var("gemini_secret")?;

    let payload_str = payload.to_string();
    let b64_payload = general_purpose::STANDARD.encode(payload_str);

    // Create a SHA384 HMAC using the secret
    let mut mac =
        Hmac::<Sha384>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(b64_payload.as_bytes());

    let signature = mac.finalize().into_bytes();

    // Convert HMAC result to a hexadecimal string
    let signature_hex = hex::encode(signature);

    // Set up HTTP client
    let client = reqwest::Client::new();

    // Create request
    match client
        .post(url)
        .header("Content-Type", "text/plain")
        .header("Content-Length", "0")
        .header("X-GEMINI-APIKEY", key)
        .header("X-GEMINI-PAYLOAD", b64_payload)
        .header("X-GEMINI-SIGNATURE", signature_hex)
        .header("Cache-Control", "no-cache")
        .send()
        .await
    {
        Ok(client) => Ok(client),
        Err(e) => {
            return Err(anyhow!(std::io::Error::new(
                ErrorKind::Other,
                format!("{}", e),
            )))
        }
    }
}
