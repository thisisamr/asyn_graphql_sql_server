use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct PaymentTransactionRequestPriceDifference {
    paymenttrasnsactionsid: Option<i32>,
    requestpricedifferencesid: Option<i32>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct PaymentTransactionRequestPriceDifferenceEdge {
    cursor: Option<i32>,
    node: PaymentTransactionRequestPriceDifference,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct PaymentTransactionRequestPriceDifferenceQueryResponse {
    page_info: PaymentTransactionRequestPriceDifferencePageInfo,
    edges: Vec<PaymentTransactionRequestPriceDifferenceEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct PaymentTransactionRequestPriceDifferencePageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_payment_transaction_request_price_difference<'ctx>(
    _self: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> PaymentTransactionRequestPriceDifferenceQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) PaymentTrasnsactionsId as paymenttrasnsactionsid,RequestPriceDifferencesId as requestpricedifferencesid from PaymentTrasnsactionRequestPriceDifference where PaymentTrasnsactionsId > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) PaymentTrasnsactionsId as paymenttrasnsactionsid,RequestPriceDifferencesId as requestpricedifferencesid from PaymentTrasnsactionRequestPriceDifference"#,
            first
        ),
    };

    let row: Vec<PaymentTransactionRequestPriceDifference> =
        sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].paymenttrasnsactionsid.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from PaymentTrasnsactionRequestPriceDifference where PaymentTrasnsactionsId > @p1")
                .bind(&curs)
                .fetch_one(pool)
                .await;
        match secondquery {
            Ok(result) => {
                if result.count > 0 {
                    hasNext = true;
                }
            }
            Err(e) => println!("{:?}", e),
        }
        PaymentTransactionRequestPriceDifferenceQueryResponse {
            page_info: PaymentTransactionRequestPriceDifferencePageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| PaymentTransactionRequestPriceDifferenceEdge {
                    cursor: Some(row.paymenttrasnsactionsid.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        PaymentTransactionRequestPriceDifferenceQueryResponse {
            page_info: PaymentTransactionRequestPriceDifferencePageInfo {
                endCursor: 0,
                hasNextPage: false,
            },
            edges: Vec::new(),
        }
    }
}
