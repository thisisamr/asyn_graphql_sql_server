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
    let mut interval: Interval = time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;
        let state = expired().await;

        match state {
            Ok(expired) => {
                if expired {
                    panic!("YOUR LICENSE HAS EXPIRED PLEASE CONTACT THE AUTHOR AMR SOLIMAN")
                } else {
                    println!("valid license 🔐");
                }
            }
            Err(e) => panic!("{:#?}", e),
        }
    }
}

async fn expired() -> Result<bool, reqwest::Error> {
    let res = reqwest::get("https://www.timeapi.io/api/TimeZone/zone?timeZone=Africa/Cairo")
        .await?
        .json::<TimeResponse>()
        .await;
    println!("{:?}", res.as_ref().unwrap().currentLocalTime);
    match res {
        Ok(time) => {
            let result = time.currentLocalTime.parse::<NaiveDateTime>().unwrap()
                > "2023-04-02T00:48:08.9930463"
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
