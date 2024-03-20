use api::gemini::{models::TradingData, orders::active_orders, process::gemini_l2_collection};
use dotenv::dotenv;
use log::error;
use tokio::time::sleep;
use std::{fs, time::Duration};


async fn read_settings(file_path: &str) -> Result<TradingData, serde_json::Error> {
    let data = fs::read_to_string(file_path).expect("Unable to read file");
    serde_json::from_str(&data)
}

#[tokio::main]
async fn main() {

    dotenv().ok();

    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();

    // let active_orders  = active_orders().await.expect("Could not get active orders");
    // println!("{:?}", active_orders);

    loop {
        let trading_data = read_settings("settings.json").await.expect("Could not find settings.dat");

        match gemini_l2_collection(trading_data).await {
            Ok(_) => (),
            Err(e) => {
                error!("{:?}", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
