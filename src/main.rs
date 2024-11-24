use binance::{api::Binance, market::*, model::{OrderBook, Symbol, Asks, Bids}};
use chrono::{DateTime, Utc};
use std::{fmt::Write, fs::{self, write, File, OpenOptions}, io::Read, thread, time::{Instant, Duration, SystemTime, UNIX_EPOCH}};
use serde::Serialize;
use csv::{Reader, StringRecord, Writer};


#[derive(Serialize)]
struct Record {
    price: f64,
    depthbids: String,
    depthasks: String,
    time: String,
}

impl Record {
    // Helper method to convert the struct fields to a Vec of strings
    fn to_record(&self) -> Vec<String> {
        vec![self.price.to_string(), self.depthbids.clone(), self.depthasks.clone() ,self.time.clone()]
    }
}


fn main() {

    //let api_key = Some("Yourapikey".into());
    //let secret_key = Some("yoursecretkey".into());
    let market: Market = Binance::new(None, None);

    let mut tick_counter = 0;

    let interval = Duration::from_secs(1);
    let mut next_tick = Instant::now() +interval;
    let mut path: String = String::from("output.csv");
    let mut csv_number = 1;

    loop {
    let mut symbol: f64 = 0.0;
    match market.get_price("BTCUSDT") {
        Ok(answer) => symbol = answer.price,
        Err(e) => println!("Error: {:?}", e),
    }
    let depth = market.get_depth("BTCUSDT").unwrap();
    tick_counter += 1;
    let timestamp = SystemTime::now();
    let datetime: DateTime<Utc> = timestamp.into();
    let formatted = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
    let depthbids = bid_to_vector(depth.bids);
    let depthasks = ask_to_vector(depth.asks);
    let temp = Record{price: symbol, depthbids: depthbids, depthasks: depthasks, time: formatted};
    if tick_counter == 2592000 {
        tick_counter = 0;
        path = new_file_for_month(csv_number);
        csv_number += 1;
    }
    write_csv(&temp, &path);

    let now = Instant::now();
    if now < next_tick {
        //println!("{:?}",next_tick - now);
        thread::sleep(next_tick - now);
        }
    next_tick += interval;
    }

    // let read = read_csv();

    // let mut ask: Vec<Bids> = Vec::new();

    // for value in read.into_iter() {
    //     let start = Instant::now();
    //     let symbol = value.price;
    //     let mut bids = vector_to_bid(&value.depthbids);
    //     let asks = vector_to_ask(&value.depthasks);
    //     ask = vector_to_bid(&value.depthbids);

    //     //print!("{}, ", bids.pop().unwrap().price);
    // }
    // for value in ask {
    // println!("{} , {}",value.price, value.qty);
    // }
}

fn ask_to_vector(asks: Vec<Asks>) -> String {
    let mut vec = String::new();
    for ask in asks {
        let str = (ask.price.to_string()+", "+ &ask.qty.to_string())+";";
        vec.write_str(&str).unwrap();
    }
    return vec;
}

fn bid_to_vector(bids: Vec<Bids>) -> String {
    let mut vec = String::new();
    for bid in bids {
        let str = (bid.price.to_string()+", "+ &bid.qty.to_string())+";";
        vec.write_str(&str).unwrap();
    }
    return vec;
}

fn vector_to_ask(str: &str) -> Vec<Asks> {
    let mut asks: Vec<Asks> = Vec::new();
    let mut str = str;

    while str != "" {
    let (before, after) = str.split_once(",").unwrap();
        str = after;
        let price_str = before.trim();
        let mut price = 0.0;
        match price_str.parse::<f64>() {
            Ok(number) => price = number,
            Err(e) => println!("{}", e),
        }
    let (before, after) = str.split_once(";").unwrap();
        str = after;
        let qty_str = before.trim();
        let mut qty = 0.0;
        match qty_str.parse::<f64>() {
            Ok(number) => qty = number,
            Err(e) => println!("{}", e),
        }

        asks.push(Asks { price: price, qty: qty });
    }
    return asks;
}

fn vector_to_bid(str: &str) -> Vec<Bids> {
    let mut bids: Vec<Bids> = Vec::new();
    let mut str = str;

    while str != "" {
    let (before, after) = str.split_once(",").unwrap();
        str = after;
        let price_str = before.trim();
        let mut price = 0.0;
        match price_str.parse::<f64>() {
            Ok(number) => price = number,
            Err(e) => println!("{}", e),
        }
    let (before, after) = str.split_once(";").unwrap();
        str = after;
        let qty_str = before.trim();
        let mut qty = 0.0;
        match qty_str.parse::<f64>() {
            Ok(number) => qty = number,
            Err(e) => println!("{}", e),
        }

        bids.push(Bids { price: price, qty: qty });
    }
    return bids;
}

fn write_csv(temp: &Record, path: &str) {

    let file = OpenOptions::new().write(true).append(true).open(path);

    match file {
        Ok(file) => {
            let mut writer = Writer::from_writer(file);
            writer.serialize(&temp.to_record()).unwrap_or_else(|_| {
                eprintln!("error serialize")});
            writer.flush().unwrap();
            }
        Err(e) => {
             eprintln!("Failed to write to file: {}", e);
        }
    }
}

fn read_csv() -> Vec<Record>{
    let mut reader = Reader::from_path("output.csv").unwrap();
    let mut records: Vec<Record> = Vec::new();

    for result in reader.records() {
        match result {
            Ok(read) => {
                let price = &read[0];
                let depthbids = read[1].to_string();
                let depthasks = read[2].to_string();
                let time = read[3].to_string();

                //println!("{} {} {} {}", price, depthbids, depthasks, time);
        
                let price = price.parse::<f64>().unwrap();
        
                let record = Record{ 
                    price: price,
                    depthasks : depthasks,
                    depthbids: depthbids,
                    time: time,
                };
                records.push(record);

            }
            Err(e) => { println!("{}", e);}
        }
    }
    return records;
}

fn new_file_for_month(number: i32) -> String {
    let path = format!("output{}.csv", number);

    let mut wtr = Writer::from_path(path.clone()).unwrap();

    wtr.flush().unwrap();

    return path
}

#[cfg(test)]
mod tests {
    use crate::new_file_for_month;

    #[test]
    fn test_new_file(){
        _ = new_file_for_month(15)
    }
}