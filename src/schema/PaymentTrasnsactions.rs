use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct PaymentTransaction {
    id: Option<i32>,
    paymenttime: Option<String>,
    merchantrefnum: Option<String>,
    price: Option<f64>,
    paymentamount: Option<f64>,
    fawryfees: Option<f64>,
    paymentmethod: Option<i32>,
    orderstatus: Option<i32>,
    referencenumber: Option<String>,
    statuscode: Option<String>,
    statusdescription: Option<String>,
    requestid: Option<i32>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    transactiontype: Option<i32>,
    refundedamount: Option<f64>,
    #[graphql(name = "sync_status")]
    sync_status: Option<i32>,
    userid: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct PaymentTransactionEdge {
    cursor: Option<i32>,
    node: PaymentTransaction,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct PaymentTransactionQueryResponse {
    page_info: PaymentTransactionPageInfo,
    edges: Vec<PaymentTransactionEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct PaymentTransactionPageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_payment_transactions<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> PaymentTransactionQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select top({})  Id as id,CAST(PaymentTime as varchar) as paymenttime,convert(nvarchar(36), MerchantRefNum) as merchantrefnum ,Price as price,PaymentAmount as paymentamount,FawryFees as fawryfees, PaymentMethod as paymentmethod,OrderStatus as orderstatus ,CAST(ReferenceNumber as nvarchar)as referencenumber,CAST(StatusCode as nvarchar) as statuscode,CAST(StatusDescription as nvarchar) as statusdescription,RequestId as requestid,Cast(AddedDate as varchar) as addeddate ,CAST(ModifiedDate as varchar) as modifieddate,cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,TransactionType as transactiontype,RefundedAmount as refundedamount, SyncStatus as sync_status,cast(UserId as nvarchar) as userid from PaymentTrasnsactions where OrderStatus=1 and Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select top({})  Id as id,CAST(PaymentTime as varchar) as paymenttime,convert(nvarchar(36), MerchantRefNum) as merchantrefnum ,Price as price,PaymentAmount as paymentamount,FawryFees as fawryfees, PaymentMethod as paymentmethod,OrderStatus as orderstatus ,CAST(ReferenceNumber as nvarchar)as referencenumber,CAST(StatusCode as nvarchar) as statuscode,CAST(StatusDescription as nvarchar) as statusdescription,RequestId as requestid,Cast(AddedDate as varchar) as addeddate ,CAST(ModifiedDate as varchar) as modifieddate,cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,TransactionType as transactiontype,RefundedAmount as refundedamount, SyncStatus as sync_status,cast(UserId as nvarchar) as userid from PaymentTrasnsactions where OrderStatus=1"#,
            first
        ),
    };

    let row: Vec<PaymentTransaction> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from PaymentTrasnsactions where Id > @p1")
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
        PaymentTransactionQueryResponse {
            page_info: PaymentTransactionPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| PaymentTransactionEdge {
                    cursor: row.id,
                    node: row,
                })
                .collect(),
        }
    } else {
        PaymentTransactionQueryResponse {
            page_info: PaymentTransactionPageInfo {
                endCursor: 0,
                hasNextPage: false,
            },
            edges: Vec::new(),
        }
    }
}
pub async fn getUpserts<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    last_sync_timestamp: String,
) -> Vec<PaymentTransaction> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select  Id as id,CAST(PaymentTime as varchar) as paymenttime,convert(nvarchar(36), MerchantRefNum) as merchantrefnum ,Price as price,PaymentAmount as paymentamount,FawryFees as fawryfees, PaymentMethod as paymentmethod,OrderStatus as orderstatus ,CAST(ReferenceNumber as nvarchar)as referencenumber,CAST(StatusCode as nvarchar) as statuscode,CAST(StatusDescription as nvarchar) as statusdescription,RequestId as requestid,Cast(AddedDate as varchar) as addeddate ,CAST(ModifiedDate as varchar) as modifieddate,cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,TransactionType as transactiontype,RefundedAmount as refundedamount, SyncStatus as sync_status,cast(UserId as nvarchar) as userid from PaymentTrasnsactions where OrderStatus=1 and ModifiedDate >='{}'"#,
                parsed_value
            );
            // get all requests the
            sqlx::query_as(&q).fetch_all(pool).await.unwrap()
        }
        Err(e) => {
            println!("{:?}", e);
            Vec::new()
        }
    }
}