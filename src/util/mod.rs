
use std::{error::Error, io};

use log::debug;


#[derive(Debug, serde::Deserialize)]
pub struct Stock {
    pub symbol:String,
    pub source:String
}

pub fn read_stocks() -> Result<Vec<Stock>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let mut stocks:Vec<Stock> = Vec::new();

    for result in rdr.deserialize() {
        let record: Stock = result?;
        debug!("{:?}", &record);
        stocks.push(record);
    }
    Ok(stocks)
}