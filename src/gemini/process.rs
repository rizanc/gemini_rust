use crate::cratesdb::{CratesDB, OneMinuteTable};
use crate::gemini::gemini_websocket::{create_v1_marketdata_ws, CandleUpdate, Heartbeat};
use anyhow::anyhow;
use anyhow::Result;

use crate::gemini::models::{Balance, GeminiOrder, OrderType};
use crate::gemini::orders::{cancel_order, get_active_orders, place_order};
use crate::util::Stock;

use chrono::Utc;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;

use std::net::TcpStream;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::RwLock;

use tungstenite::stream::MaybeTlsStream;
use tungstenite::Message;
use tungstenite::WebSocket;

use log::{debug, error, info, warn};
use tokio::time::{self, sleep, Duration};

use super::gemini_websocket::{create_order_events_ws, create_v2_marketdata_ws};
use super::models::{AccountData, Order, TradingData};
use super::orders::get_balances;

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
    let open_orders_arc = Arc::new(Mutex::new(HashMap::new()));
    let in_flight_orders_arc = Arc::new(Mutex::new(HashSet::new()));
    let last_heart_beat_arc = Arc::new(Mutex::new(Instant::now()));
    let account_data_arc = Arc::new(Mutex::new(AccountData::new()));

    // TASKS =============================================================================
    let task_hearthbeat = monitor_hearthbeat(done_arc.clone(), last_heart_beat_arc.clone());

    let task_bids_websocket_feed = bids_websocket_feed(
        trading_data_arc.read().await.symbol.clone(),
        done_arc.clone(),
        last_heart_beat_arc.clone(),
        bids_arc.clone(),
    );

    let task_order_rest_feed = order_rest_feed(
        account_data_arc.clone(),
        done_arc.clone(),
        trading_data_arc.clone(),
    );

    let task_place_orders = place_orders(
        account_data_arc.clone(),
        done_arc.clone(),
        bids_arc.clone(),
        open_orders_arc.clone(),
        in_flight_orders_arc.clone(),
        trading_data_arc.clone(),
    );

    let task_order_websocket_feed = order_websocket_feed(
        done_arc.clone(),
        open_orders_arc.clone(),
        trading_data_arc.clone(),
        in_flight_orders_arc.clone(),
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

fn order_rest_feed(
    account_data: Arc<Mutex<AccountData>>,
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
                        .filter(|o| {
                            o.symbol.to_lowercase() == symbol.to_lowercase() && o.side == "buy"
                        })
                        .collect();

                    symbol_orders.sort_by(|a, b| a.price.cmp(&b.price));

                    if symbol_orders.len() > 0 {
                        // Cancel all but ONE buy order. This will take care of duplicate orders
                        for i in 0..symbol_orders.len() - 1 {
                            info!(
                                "Cancelling order @: {:#?}",
                                symbol_orders.get(i).unwrap().price.unwrap()
                            );

                            let _ = cancel_order(
                                &symbol_orders.get(i).unwrap().order_id,
                                &Utc::now().timestamp_millis().to_string(),
                            )
                            .await;
                        }
                    } else {
                        debug!("No orders found");
                    }

                    sleep(Duration::from_millis(10000)).await;
                }
                Err(e) => {
                    error!("\n\nERROR ({:?})", e);
                    //break;
                }
            }

            match get_balances().await {
                Ok(balances) => {
                    let usd: Option<&Balance> = balances
                        .iter()
                        .find(|balance| (**balance).currency == "USD");
                    if let Some(us) = usd {
                        account_data.lock().unwrap().available = us.available;
                    } else {
                        error!("Did not find USD balance");
                    }
                }
                Err(err) => {
                    error!("{:?}", err)
                }
            }
        }
    });
    task_orders
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

                    if let Err(err) = message_loop_l2_data(
                        &mut socket,
                        &last_heart_beat_arc,
                        &bids_arc,
                        &mut counter,
                    ) {
                        if let Some(e) = err.chain().next() {
                            if let Some(te) = e.downcast_ref::<tungstenite::Error>() {
                                if matches!(te, tungstenite::Error::AlreadyClosed) {
                                    error!("{:?}", te);
                                    *done_arc.lock().unwrap() = true;
                                }
                            }
                        }

                        error!("{:?}", err);
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
    in_flight_orders_arc: Arc<Mutex<HashSet<String>>>,
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
            let done = match done_arc.lock() {
                Ok(lock) => *lock,
                Err(e) => {
                    error!("Error acquiring done lock: {}", e);
                    continue;
                }
            };

            if done {
                break;
            }

            match message_loop_orders(
                &mut socket,
                open_orders_arc.clone(),
                trading_data_arc.clone(),
                in_flight_orders_arc.clone(),
            )
            .await
            {
                Ok(_) => {
                    sleep(Duration::from_millis(1000)).await;
                }
                Err(e) => {
                    error!("ERROR -1 {:?}", e);
                    break;
                }
            };
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
    if text.len() == 0 {
        return Ok(());
    }
 
    handle_l2_message(text.to_string(), bids, counter)?;
    *last_heart_beat_clone.lock().unwrap() = Instant::now();

    Ok(())
}

async fn message_loop_orders(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    open_orders: Arc<Mutex<HashMap<String, Order>>>,
    trade_data: Arc<RwLock<TradingData>>,
    in_flight_orders_arc: Arc<Mutex<HashSet<String>>>,
) -> Result<()> {
    match socket.read() {
        Ok(msg) => {
            match handle_order_event(msg, trade_data, open_orders, in_flight_orders_arc).await {
                Ok(_) => Ok(()),
                Err(_) => Ok(()),
            }
        }
        Err(err) => Err(anyhow!("Error reading message: {}", err)),
    }
}

async fn handle_order_event(
    msg: Message,
    trade_data: Arc<RwLock<TradingData>>,
    open_orders: Arc<Mutex<HashMap<String, Order>>>,
    in_flight_orders_arc: Arc<Mutex<HashSet<String>>>,
) -> Result<()> {
    let text = msg.to_text()?;

    match serde_json::from_str::<Vec<super::models::Order>>(&text.to_string()) {
        Ok(orders) => {
            let symbol = trade_data.read().await.symbol.clone();
            let filtered_orders: Vec<&Order> = orders
                .iter()
                .filter(|order| order.symbol == symbol)
                .collect();

            let mapped_orders: Vec<(&OrderType, &String, Decimal, Decimal)> = filtered_orders
                .iter()
                .map(|order| {
                    (
                        &order.order_type,
                        &order.side,
                        order.price.unwrap_or(dec!(0)),
                        order.original_amount,
                    )
                })
                .collect();

            debug!("\n\nWEBSOCKET {symbol} ORDERS\n\n{:?}", &mapped_orders);

            remap_orders(&orders, &trade_data, &open_orders, &in_flight_orders_arc).await;

            Ok(())
        }
        Err(_) => Ok(()),
    }
}

async fn remap_orders(
    orders: &Vec<Order>,
    trade_data: &Arc<RwLock<TradingData>>,
    open_orders: &Arc<Mutex<HashMap<String, Order>>>,
    in_flight_orders_arc: &Arc<Mutex<HashSet<String>>>,
) {
    for order in orders {
        if order.symbol.to_lowercase() != trade_data.read().await.symbol.to_lowercase() {
            continue;
        }

        let order_client_id = order.client_order_id.as_ref();
        if order_client_id.is_some() {
            in_flight_orders_arc
                .lock()
                .unwrap()
                .remove(order_client_id.unwrap());
        }

        if let OrderType::Closed = order.order_type {
            open_orders.lock().unwrap().remove(&order.order_id);
        } else {
            if let OrderType::Fill = order.order_type {
                let trade_data_guard = trade_data.read().await;
                let sum = &order.original_amount;

                if order.side == "buy"
                    && order.remaining_amount.is_some()
                    && order.remaining_amount.unwrap() == dec!(0)
                {
                    let _ = place_order(&GeminiOrder {
                        client_order_id: "".to_string(),
                        symbol: trade_data_guard.symbol.clone(),
                        amount: (sum * trade_data_guard.accumulation_multiplier.clone())
                            .to_string(),
                        price: (order.clone().fill.unwrap().price
                            + trade_data_guard.order_interval.clone())
                        .to_string(),
                        side: "sell".to_string(),
                        order_type: "exchange limit".to_string(),
                        options: vec!["maker-or-cancel".to_string()],
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
}

fn place_orders(
    account_data: Arc<Mutex<AccountData>>,
    done_arc: Arc<Mutex<bool>>,
    bids_arc: Arc<Mutex<BTreeMap<Decimal, Decimal>>>,
    open_orders_arc: Arc<Mutex<HashMap<String, Order>>>,
    in_flight_orders_arc: Arc<Mutex<HashSet<String>>>,
    trade_data: Arc<RwLock<TradingData>>,
) -> tokio::task::JoinHandle<()> {
    return tokio::spawn(async move {
        // Sleep for the first 10 seconds to give time
        // for the order data to be downloaded from the
        // exchange
        time::sleep(Duration::from_millis(10000)).await;

        loop {
            if *done_arc.lock().unwrap() == true {
                break;
            }

            time::sleep(Duration::from_millis(10000)).await;

            let available = account_data.lock().unwrap().available.clone();
            info!("Available: ${:.4}", &available);
            let trade_data_guard = trade_data.read().await;

            let rt_bid = match bids_arc.lock().unwrap().iter().last() {
                Some(b) => b.0.clone(),
                None => {
                    error!("No bids found");
                    continue;
                }
            };

            let bid_delta = Decimal::new(1, trade_data_guard.decimals);

            let bids = filter_orders_by_side(&open_orders_arc, "buy");
            let offers = filter_orders_by_side(&open_orders_arc, "sell");

            let max_bid = find_max_price(&bids);
            let min_offer = find_min_price(&offers);

            let max_bid_order = if max_bid > dec!(0) {
                bids.iter()
                    .find(|(_, order)| order.price == Some(max_bid))
                    .map(|(_, order)| order.clone())
            } else {
                None
            };

            if bids.is_empty() && offers.is_empty() {
                place_limit_order(
                    &trade_data_guard,
                    rt_bid + bid_delta,
                    "buy",
                    &in_flight_orders_arc,
                )
                .await;
            } else if bids.is_empty() && !offers.is_empty() {
                handle_no_bids(
                    rt_bid,
                    min_offer,
                    bid_delta,
                    &trade_data_guard,
                    &in_flight_orders_arc,
                )
                .await;
            } else if !bids.is_empty() && !offers.is_empty() {
                handle_bids_and_offers(
                    max_bid,
                    min_offer,
                    rt_bid,
                    bid_delta,
                    &trade_data_guard,
                    max_bid_order,
                    &in_flight_orders_arc,
                )
                .await;
            } else if !bids.is_empty() && offers.is_empty() {
                handle_bids_no_offers(&max_bid_order, &rt_bid, &trade_data_guard.order_interval)
                    .await;
            }
        }
    });
}

async fn cancel_order_if_needed(order_id: String) -> Result<(), String> {
    let client_order_id = Utc::now().timestamp_millis().to_string();
    match cancel_order(&order_id, &client_order_id).await {
        Ok(_) => return Ok(()),
        Err(e) => return Err(format!("{:?}", e)),
    };
}

async fn handle_bids_no_offers(
    max_bid_order: &Option<super::models::Order>,
    rt_bid: &Decimal,
    order_interval: &Decimal,
) {
    if let Some(order) = max_bid_order {
        if let Some(price) = order.price {
            if price < (rt_bid - order_interval) {
                match cancel_order_if_needed(order.order_id.to_string()).await {
                    Ok(_) => (),
                    Err(e) => error!("{e}"),
                };
            }
        }
    }
}

async fn handle_bids_and_offers(
    max_bid: Decimal,
    min_offer: Decimal,
    rt_bid: Decimal,
    bid_delta: Decimal,
    trade_data: &TradingData,
    max_bid_order: Option<super::models::Order>,
    in_flight_orders_arc: &Arc<Mutex<HashSet<String>>>,
) {
    if max_bid > dec!(0) && min_offer > dec!(0) {
        if max_bid < min_offer - (dec!(2) * trade_data.order_interval) {
            if let Some(order) = max_bid_order {
                if let Some(price) = order.price {
                    if price < rt_bid {
                        match cancel_order_if_needed(order.order_id.to_string()).await {
                            Ok(_) => {
                                let bid_price = (rt_bid + bid_delta);

                                place_limit_order(
                                    trade_data,
                                    bid_price,
                                    "buy",
                                    in_flight_orders_arc,
                                )
                                .await;
                            }
                            Err(e) => {
                                error!("{}", e);
                            }
                        };
                    }
                }
            }
        }
    }
}

async fn handle_no_bids(
    rt_bid: Decimal,
    min_offer: Decimal,
    bid_delta: Decimal,
    trade_data: &TradingData,
    in_flight_orders_arc: &Arc<Mutex<HashSet<String>>>,
) {
    if min_offer > dec!(0) {
        let mut next_bid = min_offer - (dec!(2.0) * trade_data.order_interval);

        next_bid = next_bid.min(rt_bid + bid_delta);

        place_limit_order(trade_data, next_bid, "buy", in_flight_orders_arc).await;
    }
}
async fn place_limit_order(
    trade_data: &TradingData,
    price: Decimal,
    side: &str,
    in_flight_orders_arc: &Arc<Mutex<HashSet<String>>>,
) {
    if in_flight_orders_arc.lock().unwrap().is_empty() {
        let client_order_id = Utc::now().timestamp_millis().to_string();
        let _ = place_order(&GeminiOrder {
            client_order_id: client_order_id.clone(),
            symbol: trade_data.symbol.clone(),
            amount: trade_data.size.clone().to_string(),
            price: price.to_string(),
            side: side.to_string(),
            order_type: "exchange limit".to_string(),
            options: vec!["maker-or-cancel".to_string()],
        })
        .await;

        debug!("Placed order at {}", price);

        in_flight_orders_arc.lock().unwrap().insert(client_order_id);
    } else {
        debug!("In-flight orders are not empty.");
    }
}

fn filter_orders_by_side(
    orders_arc: &Arc<Mutex<HashMap<String, super::models::Order>>>,
    side: &str,
) -> HashMap<String, super::models::Order> {
    orders_arc.lock().map_or_else(
        |e| {
            error!("Failed to acquire lock: {:?}", e);
            HashMap::new()
        },
        |orders| {
            orders
                .iter()
                .filter(|(_, order)| order.side.eq_ignore_ascii_case(side))
                .map(|(id, order)| (id.clone(), order.clone()))
                .collect()
        },
    )
}

fn find_max_price(orders: &HashMap<String, super::models::Order>) -> Decimal {
    orders
        .values()
        .filter_map(|order| order.price)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or_default()
}

fn find_min_price(orders: &HashMap<String, super::models::Order>) -> Decimal {
    orders
        .values()
        .filter_map(|order| order.price)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or_default()
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
    let value: Value = serde_json::from_str(&message).map_err(|e| {
        error!("{message}");
        anyhow!("Error parsing JSON message: {}", e)
    })?;

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
