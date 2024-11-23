use std::{cmp, collections::HashMap};


use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TradingData {
    pub symbol: String,
    #[serde(default)]
    pub top_of_swing: Option<Decimal>,
    #[serde(default)]
    pub swing_size: Option<Decimal>,
    #[serde(default)]
    pub buy_price: Option<Decimal>,
    pub min_size: Decimal,
    pub size: Decimal,
    pub profit_spread: Decimal,
    pub order_interval: Decimal,
    // Setting this to a decimal less than one will create a smaller
    // sell order, therefore accumulation inventory.
    pub accumulation_multiplier: Decimal,
    pub decimals: u32
}

impl TradingData {

    pub fn size_getter(&self, current_price: Decimal) -> Decimal {
        let mut size = self.size;
        if let (Some(top_of_swing), Some(swing_size)) = (self.top_of_swing, self.swing_size) {
            let bottom_of_swing = top_of_swing - swing_size;

            if let (Some(bottom_of_swing_f64), Some(swing_size_f64)) =
                (bottom_of_swing.to_f64(), swing_size.to_f64())
            {
                let x_norm = 1.0
                    - ((current_price.to_f64().unwrap_or_default() - bottom_of_swing_f64)
                        / swing_size_f64);


                let a = 0.5;
                let b = 2.079;

                let mut e = a * std::f64::consts::E.powf(b * x_norm);
                e = (e * 100.0).round() / 100.0;

                size = cmp::max(
                    self.min_size,
                    Decimal::from_f64(e).unwrap_or_else(|| self.min_size),
                );
            }
        }

        size
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
