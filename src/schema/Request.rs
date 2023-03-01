use async_graphql::{Context, SimpleObject};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct RecordCount {
    count: i32,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]

pub struct PageInfo {
    endCursor: i32,
    hasNextPage: bool,
}
use super::Query;
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct Request {
    Id: i32,
    UnitType: i32,
    RequestStatus: i32,
    Area: f64,
    Price: f64,
    RequestNumber: String,
    UserId: String,
    AddedDate: String,
    ModifiedDate: String,
    SyncStatus: Option<i32>,
    HasPriceDifference: bool,
    IsPaid: bool,
    IsArchived: bool,
    SyncStatusSa: Option<i32>,
    SubUnitType: Option<i32>,
    SubUnitTypeArea: Option<f64>,
    CrewTransferCost: f64,
    StatusDescription: Option<String>,
    DeliveryDate: Option<String>,
    BuildingArea: Option<String>,
    LandArea: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct Edge {
    //cursor: i32,
    node: Request,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]

pub struct QueryResponse {
    page_info: PageInfo,
    edges: Vec<Edge>,
}
pub async fn get_all_requests<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
    date: Option<String>,
) -> QueryResponse {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    //todo
    // if date is supplied
    //for example: "2022-05-07 09:29:18.2561444"
    // we should try to parse it
    // if parsed successfully we need to construct our queries
    println!("{:?}", date);
    let q= match after{
        Some(cursor_id)=>format!("SELECT TOP ({}) Id,UnitType,RequestStatus,Area,Price,RequestNumber,UserId,CAST(AddedDate As varchar) as AddedDate,Cast(ModifiedDate As varchar) as ModifiedDate,SubUnitType,SubUnitTypeArea,LandArea,CrewTransferCost,BuildingArea ,StatusDescription,IsArchived, SyncStatusSa ,HasPriceDifference,SyncStatus,IsPaid, CAST(DeliveryDate AS varchar) as DeliveryDate from Requests where IsPaid=1 and Id >{}",first,cursor_id),

        None=>format!("SELECT TOP ({}) Id,UnitType,RequestStatus,Area,Price,RequestNumber,UserId,CAST(AddedDate As varchar) as AddedDate,Cast(ModifiedDate As varchar) as ModifiedDate,SubUnitType,SubUnitTypeArea,LandArea,CrewTransferCost,BuildingArea ,StatusDescription,IsArchived, SyncStatusSa ,HasPriceDifference,SyncStatus,IsPaid, CAST(DeliveryDate AS varchar) as DeliveryDate from Requests where IsPaid=1",first)
    };
    let row: Vec<Request> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    let mut hasNext: bool = false;
    if row.len() != 0 {
        let curs = row[row.len() - 1].Id;
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from Requests where IsPaid =1 and Id>@p1")
                .bind(curs)
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
        QueryResponse {
            page_info: PageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row.into_iter().map(|row| Edge { node: row }).collect(),
        }
    } else {
        QueryResponse {
            page_info: PageInfo {
                endCursor: 0,
                hasNextPage: false,
            },
            edges: Vec::new(),
        }
    }
}
// pub async fn get_all_by_date<'ctx>(
//     _slef: &Query::Query,
//     ctx: &Context<'ctx>,
//     date: String,
//     first: i32,
//     after: Option<i32>,
// ) -> QueryResponse {
//     todo!()
// }
