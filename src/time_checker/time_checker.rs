use actix_web::rt::time::{self, Interval};
use chrono::NaiveDateTime;
use serde::Deserialize;
use std::time::Duration;
#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
pub struct TimeResponse {
    timeZone: String,
    currentLocalTime: String,
}
pub async fn checker() {
  //  let mut interval: Interval = time::interval(Duration::from_secs(10));

  
       // interval.tick().await;
        let state = expired().await;

        match state {
            Ok(expired) => {
                if expired {
                    panic!("\n\n💀💀YOUR LICENSE HAS EXPIRED PLEASE CONTACT THE AUTHOR AMR SOLIMAN💀💀\n\n")
                } else {
                    println!("🚀🚀READY");
                }
            }
            Err(e) => panic!("\n⛔PLease make sure you are connected to the internet\n"),
        }
    
}

pub async fn expired() -> Result<bool, reqwest::Error> {
    let res = reqwest::get("https://www.timeapi.io/api/TimeZone/zone?timeZone=Africa/Cairo")
        .await?
        .json::<TimeResponse>()
        .await;
    //println!("{:?}", res.as_ref().unwrap().currentLocalTime);
    match res {
        Ok(time) => {
            time.currentLocalTime.parse::<NaiveDateTime>().unwrap();
            let result = time.currentLocalTime.parse::<NaiveDateTime>().unwrap()
                > "2023-03-21T20:05:08.9930463"
                    .parse::<NaiveDateTime>()
                    .unwrap();
            Ok(result)
        }
        Err(e) => {
            println!("{}", e);
            Ok(true)
        }
    }
}
