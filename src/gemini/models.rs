use std::collections::HashMap;


use rust_decimal::Decimal;

use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TradingData {
    pub symbol: String,
    pub size: Decimal,
    pub order_interval: Decimal,
    pub accumulation_multiplier: Decimal,
    pub decimals: u32
}

impl TradingData {

}

pub struct AccountData {
    pub available:Decimal
}

impl AccountData {
    pub fn new() -> AccountData {
        return AccountData {
            available:dec!(0)
        }
    }
}

pub struct GeminiSettings {
    pub urls: HashMap<&'static str, &'static str>,
    pub requests: HashMap<&'static str, &'static str>,
}

#[derive(Serialize, Deserialize, Debug, Clone,PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    #[serde(rename = "exchange limit")]
    ExchangeLimit,
    #[serde(rename = "subscription_ack")]
    SubscriptionAck,
    Heartbeat,
    Initial,
    Accepted,
    Rejected,
    Booked,
    Fill,
    Cancelled,
    #[serde(rename = "cancel_rejected")]
    CancelRejected,
    Closed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    #[serde(rename = "type")]
    pub order_type: OrderType,
    pub behavior: Option<String>,
    pub order_id: String,
    pub event_id: Option<String>,
    pub client_order_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub price: Option<Decimal>,
    pub original_amount: Decimal,
    pub remaining_amount: Option<Decimal>,
    pub fill: Option<Fill>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Balance {
    #[serde(rename = "type")]
    pub order_type: String,
    pub currency :String,
    pub amount: Decimal,
    pub available: Decimal,
    #[serde(rename = "availableForWithdrawal")]
    pub available_for_withdrawal: Decimal
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fill {
    pub trade_id: String,
    pub liquidity: String,
    pub price: Decimal,
    pub amount: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GeminiOrder {
    pub client_order_id: String,
    pub symbol: String,
    pub amount: String,
    pub price: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    pub options: Vec<String>,
}

impl GeminiSettings {
    pub fn new() -> GeminiSettings {
        return GeminiSettings {
            urls: HashMap::from([
                ("orders", "https://api.gemini.com/v1/orders"),
                ("open_positions", "https://api.gemini.com/v1/balances"),
                ("new_order", "https://api.gemini.com/v1/order/new"),
                ("cancel_order", "https://api.gemini.com/v1/order/cancel"),
            ]),
            requests: HashMap::from([
                ("orders", "/v1/orders"),
                ("open_positions", "/v1/balances"),
                ("new_order", "/v1/order/new"),
                ("cancel_order", "/v1/order/cancel"),
            ]),
        };
    }
}
