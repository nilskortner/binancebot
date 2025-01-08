use binance::{api::Binance, market::*, model::{OrderBook, Symbol, Asks, Bids}};
use chrono::{DateTime, Utc};
use tokio::task;
use tokio::runtime::Runtime;
use tokio::time::sleep;
use std::{fmt::Write, fs::{self, write, File, OpenOptions}, io::Read, thread, time::{Instant, Duration, SystemTime, UNIX_EPOCH}};
use serde::Serialize;
use serde_json;
use csv::{Reader, StringRecord, Writer};
use reqwest::Client;
use base64::{self, engine::general_purpose, Engine};
use std::future::{Future};
use std::task::{Context, Poll};
use std::collections::HashMap;
mod secret;


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

    let rt = tokio::runtime::Runtime::new().unwrap();

    //let api_key = Some("Yourapikey".into());
    //let secret_key = Some("yoursecretkey".into());
    let market: Market = Binance::new(None, None);

    let mut tick_counter = 0;

    let interval = Duration::from_secs(1);
    let mut path: String = String::from("output.csv");
    let mut csv_number = 1;
    let mut next_tick = Instant::now() +interval;

    loop {
    let symbol: f64;
    match market.get_price("BTCUSDT") {
        Ok(answer) => symbol = answer.price,
        Err(e) => {
            println!("Error: {:?}", e);
            let now = Instant::now();
            if now < next_tick {
                thread::sleep(next_tick - now);
                }
                next_tick += interval;
            continue
        }
    }
    let depth = match market.get_depth("BTCUSDT") {
        Ok(value) => {
            value
        }
        Err(e) => {
            println!("Error: {:?}", e);
            let now = Instant::now();
            if now < next_tick {
                thread::sleep(next_tick - now);
                }
                next_tick += interval;
            continue
        }
    };
    tick_counter += 1;
    let timestamp = SystemTime::now();
    let datetime: DateTime<Utc> = timestamp.into();
    let formatted = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
    let depthbids = bid_to_vector(depth.bids);
    let depthasks = ask_to_vector(depth.asks);
    let temp = Record{price: symbol, depthbids: depthbids, depthasks: depthasks, time: formatted};
    // 28_800
    if tick_counter == 28_800 {
        tick_counter = 0;
        let async_path = path.clone();
        rt.spawn( async { send_to_cloud_storage(async_path).await; });
        path = new_file_for_month(csv_number);
        csv_number += 1;
    }
    write_csv(&temp, &path);

    let now = Instant::now();
    if now < next_tick {
        thread::sleep(next_tick - now);
        }
    next_tick += interval;
    }
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

async fn send_to_cloud_storage(path: String){
    loop {
    let access_token = get_new_access_token().await;
    let url = "https://content.dropboxapi.com/2/files/upload";

    //println!("stcs string: {}",path);

    let content= fs::read(&path).unwrap();

    let client_result = Client::builder()
    .timeout(Duration::from_secs(300))
    .build();
    let client = match client_result {
        Ok(client_result) => {
            client_result
        }
        Err(e) => {
            println!("Request failed: {:?}", e);
            sleep(Duration::from_secs(600)).await;
            continue
        }
    };

    let dropbox_api_arg = format!(r#"{{"path":"/{}","mode":"add","autorename":false,"mute":false,"strict_conflict":false}}"#, path);

    //let client = Client::new();

    let response = client
    .post(url)
    .bearer_auth(access_token)
    .header("Dropbox-API-Arg", dropbox_api_arg)
    .header("Content-Type", "application/octet-stream")
    .body(content)
    .send()
    .await;

    match response {
        Ok(response) => {
            println!("Response: {:?}", response);
            }
        Err(e) => {
            println!("Request failed: {:?}", e);
            }
        }
    break
    }
    match fs::remove_file(&path) {
        Ok(_) => println!("success"),
        Err(e) => eprintln!("Failed to delete file: {}", e),
    }
}

async fn get_new_access_token () -> String {
    let url = "https://api.dropboxapi.com/oauth2/token";

    loop {

    let client_result = Client::builder()
    .timeout(Duration::from_secs(60))
    .build();
    let client = match client_result {
        Ok(client_result) => {
            client_result
        }
        Err(e) => {
            println!("Request failed: {:?}", e);
            //3600
            sleep(Duration::from_secs(600)).await;
            continue
        }
    };

    let credentials = format!("{}:{}", secret::ID, secret::SECRET);
    let encoded_credentials = general_purpose::STANDARD.encode(credentials);

    let response = client
    .post(url)
    .header("Authorization", format!("Basic {}", encoded_credentials))
    .form(&[("grant_type", "refresh_token"),("refresh_token", secret::REFRESH)])
    .send()
    .await;

    match response {
        Ok( response) => {
        let body = response.text().await;
        match body {
                Ok(body) => {
                    let json: HashMap<String, serde_json::Value> = serde_json::from_str(&body).unwrap();
                    println!("Response: {:?}", json);
                    return json.get("access_token").unwrap().as_str().unwrap_or("fail").to_string();
                }
                Err(e) => {
                    println!("Request failed: {:?}", e);
                    sleep(Duration::from_secs(3600)).await;
                    continue;
                }
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            sleep(Duration::from_secs(3600)).await;
            continue;
        }
    }
    }
}

fn poll_async<F>(future: F)
where
    F: Future,
    F::Output: std::fmt::Debug, 
{
    let mut future = Box::pin(future);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(result) => {
            println!("Completed with: {:?}", result);
            break;},
        Poll::Pending => {
            print!("still pending");
            std::thread::sleep(std::time::Duration::from_millis(100));
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, PollFn};

    use crate::{get_new_access_token, new_file_for_month, poll_async, send_to_cloud_storage};

    #[test]
    fn test_new_file(){
        _ = new_file_for_month(15)
    }

    #[tokio::test]
    async fn test_cloud_storage() {
        let str: String= "output.csv".to_string();
        send_to_cloud_storage(str).await;
    }

    #[tokio::test]
    async fn test_get_token() {
        let str = get_new_access_token().await;
        println!("{}",str);
    }
}