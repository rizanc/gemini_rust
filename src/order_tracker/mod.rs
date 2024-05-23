use std::{collections::VecDeque, fmt::Debug, io::Write, str::FromStr};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackedOrder {
    pub price: Decimal,
    pub amount: Decimal,
}

impl FromStr for TrackedOrder {
    type Err = std::num::ParseFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        Ok(TrackedOrder {
            price: Decimal::from_str_exact(parts[0]).unwrap(),
            amount: Decimal::from_str_exact(parts[1]).unwrap(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrderTracker<T> {
    pub symbol: String,
    pub buy_orders: VecDeque<T>,
}

impl<T> OrderTracker<T>
where
    T: Debug + Serialize + DeserializeOwned,
{
    pub fn new(symbol: &str) -> OrderTracker<T> {
        OrderTracker {
            symbol: symbol.to_string(),
            buy_orders: VecDeque::new(),
        }
    }

    pub fn add_order(&mut self, order: T) {
        self.buy_orders.push_back(order);
    }

    pub fn remove_order(&mut self) -> Option<T> {
        self.buy_orders.pop_back()
    }

    pub fn peek_order(&self) -> Option<&T> {
        self.buy_orders.back()
    }

    pub fn print_orders(&self) {
        for order in &self.buy_orders {
            println!("{:?}", order);
        }
    }

    // save as json file
    pub fn save_orders_to_file(&self) {
        let file_name = format!("{}.json", self.symbol);
        let mut file = std::fs::File::create(file_name).unwrap();
        let json = serde_json::to_string(&self.buy_orders).unwrap();
        file.write_all(json.as_bytes()).unwrap();
    }

    pub fn load_orders_from_file(&mut self) {
        let file_name = format!("{}.json", self.symbol);
        
        let file = match std::fs::File::open(file_name) {
            Ok(file) => file,
            Err(_) => return
        };
        
        let orders: Vec<T> = serde_json::from_reader(file).unwrap();
        self.buy_orders = VecDeque::from(orders);
    }
}

 #[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn test_order_tracker() {
        let mut order_tracker = OrderTracker::<TrackedOrder>::new("BTCUSD");

        order_tracker.load_orders_from_file();

        // order_tracker.add_order(Order { price: 100.0, amount: 1.0 });
        // order_tracker.add_order(Order { price: 200.0, amount: 2.0 });
        // order_tracker.add_order(Order { price: 300.0, amount: 3.0 });

        dbg!(&order_tracker);
        order_tracker.save_orders_to_file();

        assert_eq!(order_tracker.peek_order().unwrap().price, dec!(300.0));

        order_tracker.remove_order();
        assert_eq!(order_tracker.peek_order().unwrap().price, dec!(200.0));

        order_tracker.remove_order();
        assert_eq!(order_tracker.peek_order().unwrap().price, dec!(100.0));

        order_tracker.remove_order();
        assert_eq!(order_tracker.peek_order(), None);

        // let mut new_order_tracker = OrderTracker::<Order>::new("BTCUSD");
        // new_order_tracker.load_orders_from_file();
        // assert_eq!(new_order_tracker.peek_order().unwrap().price, 100.0);
    }
}

