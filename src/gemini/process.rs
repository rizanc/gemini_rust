use crate::cratesdb::{CratesDB, OneMinuteTable};
use crate::gemini::gemini_websocket::{create_v1_marketdata_ws, CandleUpdate, Heartbeat};
use anyhow::anyhow;
use anyhow::Result;
use serde::de;

use crate::gemini::models::{GeminiOrder, OrderType};
use crate::gemini::orders::{cancel_order, get_active_orders, place_order};
use crate::util::Stock;

use chrono::Utc;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;

use std::net::TcpStream;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::RwLock;

use tungstenite::stream::MaybeTlsStream;
use tungstenite::Message;
use tungstenite::WebSocket;

use log::{debug, error, warn};
use tokio::time::{self, sleep, Duration};

use super::gemini_websocket::{create_order_events_ws, create_v2_marketdata_ws};
use super::models::{Order, TradingData};

use thiserror::Error;

// Define a custom error type
#[derive(Error, Debug)]
pub enum GeminiError {
    #[error("data not found")]
    NotFound,
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("internal error")]
    Internal,
    #[error(transparent)]
    IoError(#[from] std::io::Error), // Automatically convert from std::io::Error
}

pub async fn gemini_market_maker(trading_data: TradingData) -> Result<()> {
    let trading_data_arc = Arc::new(RwLock::new(trading_data));
    let bids_arc = Arc::new(Mutex::new(BTreeMap::new()));
    let done_arc = Arc::new(Mutex::new(false));
    let post_orders_arc = Arc::new(Mutex::new(true));
    let open_orders_arc = Arc::new(Mutex::new(HashMap::new()));
    let last_heart_beat_arc = Arc::new(Mutex::new(Instant::now()));

    // TASKS =============================================================================
    let task_hearthbeat = monitor_hearthbeat(done_arc.clone(), last_heart_beat_arc.clone());

    let task_place_orders = place_orders(
        done_arc.clone(),
        post_orders_arc.clone(),
        bids_arc.clone(),
        open_orders_arc.clone(),
        trading_data_arc.clone(),
    );

    let task_bids_websocket_feed = bids_websocket_feed(
        trading_data_arc.read().await.symbol.clone(),
        done_arc.clone(),
        last_heart_beat_arc.clone(),
        bids_arc.clone(),
    );

    let task_order_rest_feed = order_rest_feed(
        done_arc.clone(),
        trading_data_arc.clone(),
    );

    let task_order_websocket_feed = order_websocket_feed(
        done_arc.clone(),
        open_orders_arc.clone(),
        trading_data_arc.clone(),
    );

    match tokio::select! {
        result = task_hearthbeat => result,
        result = task_bids_websocket_feed => result,
        result = task_order_websocket_feed => result,
        result = task_order_rest_feed => result,
        result = task_place_orders => result
    } {
        Ok(_) => {
            *(done_arc).lock().unwrap() = true;
            Ok(())
        }
        Err(_) => {
            *(done_arc).lock().unwrap() = true;
            Err(anyhow!("Error in task"))
        }
    }
}

fn bids_websocket_feed(
    symbol: String,
    done_arc: Arc<Mutex<bool>>,
    last_heart_beat_arc: Arc<Mutex<Instant>>,
    bids_arc: Arc<Mutex<BTreeMap<Decimal, Decimal>>>,
) -> tokio::task::JoinHandle<()> {
    let task_market_data = tokio::spawn(async move {
        match create_v1_marketdata_ws(&symbol) {
            Ok((mut socket, _)) => {
                let mut counter: u64 = 0;
                loop {
                    let done = {
                        let done_lock = done_arc.lock();
                        match done_lock {
                            Ok(lock) => *lock,
                            Err(e) => {
                                error!("Error acquiring done lock: {}", e);
                                continue;
                            }
                        }
                    };

                    if done {
                        break;
                    }

                    if let Err(e) = message_loop_l2_data(
                        &mut socket,
                        &last_heart_beat_arc,
                        &bids_arc,
                        &mut counter,
                    ) {
                        error!("{:?}", e);
                        break;
                    }
                }
            }
            Err(e) => {
                error!("{}", e);
            }
        }
    });

    task_market_data
}

fn order_websocket_feed(
    done_arc: Arc<Mutex<bool>>,
    open_orders_arc: Arc<Mutex<HashMap<String, Order>>>,
    trading_data_arc: Arc<RwLock<TradingData>>,
) -> tokio::task::JoinHandle<()> {
    let task_orders = tokio::spawn(async move {
        let mut socket = match create_order_events_ws(&json!({
            "request": "/v1/order/events",
            "nonce": Utc::now().timestamp_millis().to_string()
        }))
        .await
        {
            Ok((socket, _)) => socket,
            Err(e) => {
                error!("Error creating order events websocket: {}", e);
                return;
            }
        };

        loop {
            let done = {
                let done_lock = done_arc.lock();
                match done_lock {
                    Ok(lock) => *lock,
                    Err(e) => {
                        error!("Error acquiring done lock: {}", e);
                        continue;
                    }
                }
            };

            if done {
                break;
            }

            match message_loop_orders(
                &mut socket,
                open_orders_arc.clone(),
                trading_data_arc.clone(),
            )
            .await
            {
                Ok(_) => {
                    sleep(Duration::from_millis(1000)).await;
                }
                Err(e) => {
                    error!("{:?}", e);
                    break;
                }
            };
        }
    });
    task_orders
}

fn order_rest_feed(
    done_arc: Arc<Mutex<bool>>,
    trading_data_arc: Arc<RwLock<TradingData>>,
) -> tokio::task::JoinHandle<()> {
    let task_orders = tokio::spawn(async move {
        loop {
            let done = {
                let done_lock = done_arc.lock();
                match done_lock {
                    Ok(lock) => *lock,
                    Err(e) => {
                        error!("Error acquiring done lock: {}", e);
                        continue;
                    }
                }
            };

            if done {
                break;
            }

            match get_active_orders().await {
                Ok(orders) => {
                    let symbol = trading_data_arc.read().await.symbol.clone();
                    let mut symbol_orders: Vec<Order> = orders
                        .into_iter()
                        .filter(|o| o.symbol.to_lowercase() == symbol.to_lowercase() && o.side == "buy")
                        .collect();
                        
                        symbol_orders.sort_by(|a,b| a.price.cmp(&b.price));


                    // Cancel all but ONE buy order. This will take care of duplicate orders
                    for i in 0..symbol_orders.len() -1  {
                        debug!("Cancelling order @: {:#?}", symbol_orders.get(i).unwrap().price.unwrap());
                        let _ = cancel_order(&symbol_orders.get(i).unwrap().order_id).await;
                    }
                    
                    //debug!("{}", serde_json::to_string_pretty(&symbol_orders).unwrap());
                    sleep(Duration::from_millis(10000)).await;
                }
                Err(e) => {
                    error!("{:?}", e);
                    break;
                }
            }
        }
    });
    task_orders
}

pub async fn gemini_one_minute_collection(stocks: &Vec<&Stock>) -> Result<(), Box<dyn Error>> {
    let bids: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    let bids_arc = Arc::new(Mutex::new(bids));

    let last_heart_beat = Arc::new(Mutex::new(Instant::now()));
    let stock_symbols: Vec<String> = stocks.iter().map(|s| s.symbol.clone()).collect();

    let last_heart_beat_clone = last_heart_beat.clone();
    let bids_arc_clone = bids_arc.clone();

    let (mut socket, _) = create_v2_marketdata_ws()?;

    for stock in stock_symbols {
        subscribe_to_symbol(&mut socket, &stock)?;
    }

    socket.flush()?;

    tokio::spawn(async move {
        let mut counter = 0;

        loop {
            dbg!(counter);
            let _ = message_loop_l2_data(
                &mut socket,
                &last_heart_beat_clone,
                &bids_arc_clone,
                &mut counter,
            );
        }
    })
    .await?;

    Ok(())
}

fn message_loop_l2_data(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    last_heart_beat_clone: &Arc<Mutex<Instant>>,
    bids: &Arc<Mutex<BTreeMap<Decimal, Decimal>>>,
    counter: &mut u64,
) -> Result<()> {
    let msg = socket.read()?;
    let text = msg.to_text()?;
    handle_l2_message(text.to_string(), bids, counter)?;
    *last_heart_beat_clone.lock().unwrap() = Instant::now();
    Ok(())
}

async fn message_loop_orders(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    open_orders: Arc<Mutex<HashMap<String, Order>>>,
    trade_data: Arc<RwLock<TradingData>>,
) -> Result<()> {
    match socket.read() {
        Ok(msg) => {
            handle_orders(msg, trade_data, open_orders).await;
            Ok(())
        }
        Err(err) => Err(anyhow!("Error reading message: {}", err)),
    }
}

async fn handle_orders(
    msg: Message,
    trade_data: Arc<RwLock<TradingData>>,
    open_orders: Arc<Mutex<HashMap<String, Order>>>,
) {
    if let Ok(text) = msg.to_text() {
        match serde_json::from_str::<Vec<super::models::Order>>(&text.to_string()) {
            Ok(orders) => {
                remap_orders(&orders, &trade_data, &open_orders).await;
            }
            Err(_) => {}
        }
    }
}

async fn remap_orders(orders: &Vec<Order>, trade_data: &Arc<RwLock<TradingData>>, open_orders: &Arc<Mutex<HashMap<String, Order>>>) {
    let mut minimum_order_price: Option<Decimal> = None;

    for order in orders {
        if order.symbol.to_lowercase() != trade_data.read().await.symbol.to_lowercase()
        {
            continue;
        }

        if order.side == "sell" {
            if let Some(price) = order.price {
                minimum_order_price = match minimum_order_price {
                    Some(minimum_price) => Some(std::cmp::min(price, minimum_price)),
                    None => Some(price),
                };
            }
        }

        if let OrderType::Closed = order.order_type {
            open_orders.lock().unwrap().remove(&order.order_id);
        } else {
            if let OrderType::Fill = order.order_type {
                {
                    let order_interval = trade_data.read().await.order_interval.clone();
                    trade_data.write().await.buy_price =
                        Some(order.clone().fill.unwrap().price - order_interval);
                }

                let trade_data_guard = trade_data.read().await;

                let sum = &order.original_amount;

                if order.side == "buy" && order.remaining_amount == dec!(0) {
                    let _ = place_order(&GeminiOrder {
                        client_order_id: "".to_string(),
                        symbol: trade_data_guard.symbol.clone(),
                        amount: (sum
                            * trade_data_guard.accumulation_multiplier.clone())
                        .to_string(),
                        price: (order.clone().fill.unwrap().price
                            + trade_data_guard.profit_spread.clone())
                        .to_string(),
                        side: "sell".to_string(),
                        order_type: "exchange limit".to_string(),
                        options: vec![],
                    })
                    .await;
                }
            }

            open_orders
                .lock()
                .unwrap()
                .insert(order.order_id.clone(), order.clone());
        }
    }

    if let Some(price) = minimum_order_price {
        {
            let order_interval = trade_data.read().await.order_interval.clone();
            let profit_spread = trade_data.read().await.profit_spread.clone();
            trade_data.write().await.buy_price =
                Some(price - (order_interval + profit_spread));

            debug!(
                "Lowest Sell Order {} | Next Buy Order Price = {}",
                price,
                price - (order_interval + profit_spread)
            );
        }
    }
}

fn place_orders(
    done_arc: Arc<Mutex<bool>>,
    place_orders: Arc<Mutex<bool>>,
    bids_arc: Arc<Mutex<BTreeMap<Decimal, Decimal>>>,
    open_orders_arc: Arc<Mutex<HashMap<String, Order>>>,
    trade_data: Arc<RwLock<TradingData>>,
) -> tokio::task::JoinHandle<()> {
    return tokio::spawn(async move {
        // Sleep for the first 10 seconds to give time
        // for the order data to be downloaded from the
        // exchange
        time::sleep(Duration::from_millis(10000)).await;

        let mut placed_orders: HashMap<Decimal, bool> = HashMap::new();

        loop {
            if *done_arc.lock().unwrap() == true {
                break;
            }
            time::sleep(Duration::from_millis(5000)).await;

            let monitor: bool;
            let trade_data_guard = trade_data.read().await;
            let _guard = match place_orders.lock() {
                Ok(f) => monitor = *f,
                Err(_) => {
                    debug!("Failed to acquire lock on place_orders");
                    continue;
                }
            };

            if monitor {
                let bid: Decimal;

                if let Ok(ref bid_guard) = bids_arc.lock() {
                    debug!("Current Bid: {:?}", bid_guard);
                }

                match bids_arc.lock().unwrap().iter().last() {
                    Some(b) => bid = b.0.clone(),
                    None => {
                        error!("No bids found");
                        continue;
                    }
                }

                let mut found: bool = false;
                let mut orders_to_cancel: Vec<String> = Vec::new();

                let mut open_orders: HashMap<String, super::models::Order> = HashMap::new();

                let _guard = match open_orders_arc.lock() {
                    Ok(d) => {
                        for (order_id, order) in d.iter() {
                            if order.symbol.to_lowercase() == trade_data_guard.symbol.to_lowercase()
                            {
                                open_orders.insert(order_id.clone(), order.clone());
                            }
                        }
                    }

                    Err(_) => todo!(),
                };

                //debug!("{}", serde_json::to_string_pretty(&open_orders).unwrap());
                for (_, order) in open_orders.iter() {
                    if order.symbol.to_lowercase() == trade_data_guard.symbol.to_lowercase()
                        && order.side == "buy"
                    {
                        if let Some(ref price) = order.price {
                            placed_orders.remove(price);
                        }

                        found = true;

                        if let Some(price) = order.price {
                            if bid > Decimal::new(0, 0) && (bid - price) > dec!(0.05) {
                                let tgt_price = trade_data.read().await.buy_price;

                                if tgt_price.map_or(true, |tp| tp > bid || tp > price + dec!(0.05))
                                {
                                    orders_to_cancel.push(order.order_id.clone());
                                    found = false;
                                }
                            }
                        }
                    }
                }

                for order_id in orders_to_cancel {
                    let _ = cancel_order(&order_id).await;
                }

                match trade_data.read().await.buy_price {
                    Some(min_price) => min_price.to_string(),
                    None => format!("None"),
                };

                if !found {
                    let buy_price = match trade_data_guard.buy_price.clone() {
                        Some(price) => price,
                        None => bid,
                    };

                    debug!("Buy Price {} ", buy_price);

                    let buy_price = cmp::min(buy_price, bid);

                    let order_buy_price = buy_price + dec!(.01);

                    if placed_orders.is_empty() {
                        let _ = place_order(&GeminiOrder {
                            client_order_id: "".to_string(),
                            symbol: trade_data_guard.symbol.clone(),
                            amount: trade_data_guard
                                .size_getter(order_buy_price)
                                .clone()
                                .to_string(),
                            price: order_buy_price.to_string(),
                            side: "buy".to_string(),
                            order_type: "exchange limit".to_string(),
                            options: vec!["maker-or-cancel".to_string()],
                        })
                        .await;

                        debug!("Placed order at {}", order_buy_price);

                        placed_orders.insert(order_buy_price, true);
                    }
                }
            }
        }
    });
}

fn monitor_hearthbeat(
    done_arc: Arc<Mutex<bool>>,
    last_heart_beat: Arc<Mutex<Instant>>,
) -> tokio::task::JoinHandle<()> {
    let last_heart_beat_clone = last_heart_beat.clone();
    let t2 = tokio::spawn(async move {
        loop {
            if *done_arc.lock().unwrap() {
                break;
            }
            time::sleep(Duration::from_secs(10)).await;
            let last_heart_beat = *last_heart_beat_clone.lock().unwrap();
            if Instant::now().duration_since(last_heart_beat) > Duration::from_secs(30) {
                error!("No heartbeat in 30 seconds");
                break;
            }
        }
    });
    t2
}

fn subscribe_to_symbol(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    symbol: &str,
) -> Result<(), Box<dyn Error>> {
    let message = format!(
        r#"{{"type": "subscribe","subscriptions":[{{"name":"candles_30m","symbols":["{}"]}}]}}"#,
        symbol
    );

    socket.write(Message::text(message))?;
    Ok(())
}

fn _subscribe_to_l2(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    symbol: &str,
) -> Result<(), Box<dyn Error>> {
    let message = format!(
        r#"{{"type": "subscribe","subscriptions":[{{"name":"l2","symbols":["{}"]}}]}}"#,
        symbol
    );

    socket.write(Message::text(message))?;
    Ok(())
}

fn _handle_message(
    message: String,
    crates_db: &mut CratesDB,
    bids: &Arc<Mutex<BTreeMap<Decimal, Decimal>>>,
) -> Result<(), Box<dyn Error>> {
    let value: Value = serde_json::from_str(&message).map_err(|e| {
        error!("Error parsing JSON message: {}", e);
        error!("Message: {}", message);

        e
    })?;

    match value["type"].as_str() {
        Some("update") => {
            while let Some(events) = value["events"].as_array() {
                for event in events {
                    if let Some(event_obj) = event.as_object() {
                        if let (Some(price), Some(side), Some(remaining), Some(ev_type)) = (
                            event_obj["price"].as_str(),
                            event_obj["side"].as_str(),
                            event_obj["remaining"].as_str(),
                            event_obj["type"].as_str(),
                        ) {
                            if ev_type == "change" && side == "bid" {
                                if let Ok(remaining_value) = Decimal::from_str(remaining) {
                                    if remaining_value == dec!(0.0) {
                                        bids.lock()
                                            .unwrap()
                                            .remove_entry(&Decimal::from_str(price).unwrap());
                                    } else {
                                        bids.lock().unwrap().insert(
                                            Decimal::from_str(price).unwrap(),
                                            remaining_value,
                                        );
                                    }
                                } else {
                                    error!("Error parsing remaining value");
                                }
                            }
                        } else {
                            error!("Missing required fields in event");
                        }
                    }
                }
            }
        }
        Some("candles_1m_updates") => {
            let update: CandleUpdate = serde_json::from_value(value).map_err(|e| {
                error!("Error parsing Candle Update: {}", e);
                e
            })?;
            for change in update.changes {
                crates_db.upsert(OneMinuteTable {
                    ts: change[0],
                    open: change[1],
                    high: change[2],
                    low: change[3],
                    close: change[4],
                    vol: change[5],
                    symbol: update.symbol.clone(),
                })?;
            }
        }
        Some("heartbeat") => {
            let _: Heartbeat = serde_json::from_value(value).map_err(|e| {
                error!("Error parsing Heartbeat: {}", e);
                e
            })?;
        }
        _ => warn!("Unknown message type"),
    }

    Ok(())
}

fn handle_l2_message(
    message: String,
    bids: &Arc<Mutex<BTreeMap<Decimal, Decimal>>>,
    counter: &mut u64,
) -> Result<()> {
    let value: Value =
        serde_json::from_str(&message).map_err(|e| anyhow!("Error parsing JSON message: {}", e))?;

    let compare = value["socket_sequence"].as_u64().unwrap();
    *counter = compare;
    if compare > (*counter + 1) {
        return Err(anyhow!("Counter error"));
    }

    match value["type"].as_str() {
        Some("update") => {
            let events = value["events"].as_array().unwrap();
            for ref event in events {
                if let Some(event_obj) = event.as_object() {
                    if let (Some(price), Some(side), Some(remaining), Some(ev_type)) = (
                        event_obj["price"].as_str(),
                        event_obj["side"].as_str(),
                        event_obj["remaining"].as_str(),
                        event_obj["type"].as_str(),
                    ) {
                        if ev_type == "change" && side == "bid" {
                            if let Ok(remaining_value) = Decimal::from_str(remaining) {
                                bids.lock().unwrap().clear();
                                bids.lock()
                                    .unwrap()
                                    .insert(Decimal::from_str(price).unwrap(), remaining_value);
                            } else {
                                error!("Error parsing remaining value");
                            }
                        }
                    } else {
                        return Err(anyhow!("Missing required fields in event"));
                    }
                }
            }
        }
        Some("heartbeat") => {
            let _: Heartbeat = serde_json::from_value(value)
                .map_err(|e| anyhow!("Error parsing Heartbeat: {}", e))?;
        }
        _ => warn!("Unknown message type"),
    }

    Ok(())
}
