use chrono::Utc;
use log::debug;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    error::Error,
    io::{self, ErrorKind},
};

use super::{
    gemini_core::*,
    models::{GeminiOrder, GeminiSettings},
};

use anyhow::anyhow;
use anyhow::Result;


#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub symbol: String,
    pub original_amount: String,
    pub side: String,
    pub price: String,
    pub executed_amount: String,
}

pub async fn get_active_orders() -> Result<Vec<crate::gemini::models::Order>>{

    let response = post(
        "https://api.gemini.com/v1/orders",
        &json!({
            "nonce": Utc::now().timestamp_millis().to_string(),
            "request": "/v1/orders",
        }),
    )
    .await?;

    let orders = response.text().await?;

    Ok(serde_json::from_str::<Vec<crate::gemini::models::Order>>(&orders)?)
}

pub async fn place_order(order_send: &GeminiOrder) -> Result<Order, Box<dyn std::error::Error>> {
    let settings = GeminiSettings::new();

    debug!("{}", serde_json::to_string_pretty(order_send)?);

    let order_data = json!({
        "client_order_id": order_send.client_order_id,
        "request": settings.requests["new_order"],
        "nonce": Utc::now().timestamp_millis().to_string(),
        "symbol":order_send.symbol,
        "amount":order_send.amount,
        "price":order_send.price,
        "side":order_send.side,
        "type":order_send.order_type,
        "options": order_send.options // ["maker-or-cancel"]
    });

    debug!(
        "\n\nPlacing Order {}\n\n",
        &order_data
    );


    let response = post(
        settings.urls["new_order"],
        &order_data,
    )
    .await?;

    let result = serde_json::from_str::<Order>(&response.text().await?)?;

    debug!("\n\nResponse {:?}\n\n",&result);

    Ok(result)

}

pub async fn cancel_order(order_id: &str) -> Result<Order, Box<dyn std::error::Error>> {
    let settings = GeminiSettings::new();
    let response = post(
        settings.urls["cancel_order"],
        &json!({
            "request": "/v1/order/cancel".to_string(),
            "nonce": Utc::now().timestamp_millis().to_string(),
            "order_id":order_id.to_string()
        }),
    )
    .await?;

    Ok(serde_json::from_str::<Order>(&response.text().await?)?)
}

pub async fn active_orders() -> Result<Vec<Order>, Box<dyn Error>> {
    let settings = GeminiSettings::new();
    let response = post(
        settings.urls["orders"],
        &json!({
            "request": settings.requests["orders"],
            "nonce": Utc::now().timestamp_millis().to_string(),
        }),
    )
    .await?;

    if response.status().is_success() {
        let response_body = response.text().await?;
        serde_json::from_str::<Vec<Order>>(&response_body)
            .map_err(|e| format!("Failed to parse JSON: {:?}", e).into())
    } else {
        let error_message = response.text().await?;
        Err(format!("Error response from server: {}", error_message).into())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct OpenPosition {
    #[serde(rename = "type")]
    pub pos_type: String,
    pub currency: String,
    pub amount: Decimal,
    pub available: Decimal,
    pub available_for_withdrawal: Decimal,
}

pub async fn open_positions() -> Result<Vec<OpenPosition>, Box<dyn Error>> {
    let settings = GeminiSettings::new();
    let response = post(
        settings.urls["open_positions"],
        &json!({
            "request": settings.requests["open_positions"],
            "nonce": Utc::now().timestamp_millis().to_string(),
            "account":"Primary"
        }),
    )
    .await?;

    if response.status().is_success() {
        let my_positions: Vec<OpenPosition> = response.json().await.expect("Failed to parse JSON");
        Ok(my_positions)
    } else {
        let error_message = format!("Request failed with status: {}", response.status());
        Err(Box::new(io::Error::new(ErrorKind::Other, error_message)))
    }
}
