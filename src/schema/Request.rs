use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
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
    id: i32,
    unittype: Option<i32>,
    requeststatus: Option<i32>,
    area: Option<f64>,
    areatype: Option<i32>,
    price: Option<f64>,
    requestnumber: Option<String>,
    createdby: Option<String>,
    userid: Option<String>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    syncstatus: Option<i32>,
    haspricedifference: Option<bool>,
    ispaid: Option<bool>,
    isarchived: Option<bool>,
    syncstatussa: Option<i32>,
    subunittype: Option<i32>,
    subunittypearea: Option<f64>,
    crewtransfercost: Option<f64>,
    statusdescription: Option<String>,
    deliverydate: Option<String>,
    buildingarea: Option<String>,
    landarea: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct Edge {
    cursor: Option<i32>,
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
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let mut datequery: Option<String> = None;
    if let Some(date) = date {
        let parsed = NaiveDateTime::parse_from_str(date.trim(), "%Y-%m-%d %H:%M:%S%.f");
        if parsed.is_ok() {
            datequery = Some(date);
        } else {
            datequery = None;
        }
    };
    let q = match datequery {
        Some(datefilter) => {
            let q = match after {
                Some(cursor_id) => format!(
                    r#"SELECT TOP ({}) Id as id,
                UnitType as unittype,
                RequestStatus as requeststatus,
                Area as area,
                AreaType as areatype,
                Price as price,
                RequestNumber as requestnumber,
                Cast(Createdby AS varchar) as createdby,
                UserId as userid,
                CAST(AddedDate As varchar) as addeddate,
                Cast(ModifiedDate As varchar) as modifieddate,
                SubUnitType as subunittype,
                SubUnitTypeArea as subunittypearea,
                LandArea as landarea,
                CrewTransferCost as crewtransfercost,
                BuildingArea as buildingarea,
                StatusDescription as statusdescription,
                IsArchived as isarchived ,
                SyncStatusSa as syncstatussa,
                HasPriceDifference as haspricedifference ,
                SyncStatus as syncstatus ,
                IsPaid as ispaid ,
                Cast(CAST(DeliveryDate AS date)as varchar) as deliverydate from Requests where IsPaid=1 and Id >{} and AddedDate > '{}'"#,
                    first, cursor_id, datefilter
                ),

                None => format!(
                    r#"SELECT TOP ({}) Id as id,
                UnitType as unittype,
                RequestStatus as requeststatus,
                Area as area,
                AreaType as areatype,
                Price as price,
                RequestNumber as requestnumber,
                Cast(Createdby AS varchar) as createdby,
                UserId as userid,
                CAST(AddedDate As varchar) as addeddate,
                Cast(ModifiedDate As varchar) as modifieddate,
                SubUnitType as subunittype,
                SubUnitTypeArea as subunittypearea,
                LandArea as landarea,
                CrewTransferCost as crewtransfercost,
                BuildingArea as buildingarea,
                StatusDescription as statusdescription,
                IsArchived as isarchived ,
                SyncStatusSa as syncstatussa,
                HasPriceDifference as haspricedifference ,
                SyncStatus as syncstatus ,
                IsPaid as ispaid ,
                Cast(CAST(DeliveryDate AS date) as varchar) as deliverydate from Requests where IsPaid=1and AddedDate > '{}'"#,
                    first, datefilter
                ),
            };
            q
        }
        None => {
            let q = match after {
                Some(cursor_id) => format!(
                    r#"SELECT TOP ({}) Id as id,
                UnitType as unittype,
                RequestStatus as requeststatus,
                Area as area,
                AreaType as areatype,
                Price as price,
                RequestNumber as requestnumber,
                Cast(Createdby AS varchar) as createdby,
                UserId as userid,
                CAST(AddedDate As varchar) as addeddate,
                Cast(ModifiedDate As varchar) as modifieddate,
                SubUnitType as subunittype,
                SubUnitTypeArea as subunittypearea,
                LandArea as landarea,
                CrewTransferCost as crewtransfercost,
                BuildingArea as buildingarea,
                StatusDescription as statusdescription,
                IsArchived as isarchived ,
                SyncStatusSa as syncstatussa,
                HasPriceDifference as haspricedifference ,
                SyncStatus as syncstatus ,
                IsPaid as ispaid ,
                Cast(CAST(DeliveryDate AS date) as varchar) as deliverydate from Requests where IsPaid=1 and Id >{}"#,
                    first, cursor_id
                ),

                None => format!(
                    r#"SELECT TOP ({}) Id as id,
                UnitType as unittype,
                RequestStatus as requeststatus,
                Area as area,
                AreaType as areatype,
                Price as price,
                RequestNumber as requestnumber,
                Cast(Createdby AS varchar) as createdby,
                UserId as userid,
                CAST(AddedDate As varchar) as addeddate,
                Cast(ModifiedDate As varchar) as modifieddate,
                SubUnitType as subunittype,
                SubUnitTypeArea as subunittypearea,
                LandArea as landarea,
                CrewTransferCost as crewtransfercost,
                BuildingArea as buildingarea,
                StatusDescription as statusdescription,
                IsArchived as isarchived ,
                SyncStatusSa as syncstatussa,
                HasPriceDifference as haspricedifference ,
                SyncStatus as syncstatus ,
                IsPaid as ispaid ,
                Cast(CAST(DeliveryDate AS date) as varchar) as deliverydate from Requests where IsPaid=1"#,
                    first
                ),
            };
            q
        }
    };
    let row: Vec<Request> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id;
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
        // let asd: Vec<Edge> = row
        //     .clone()
        //     .into_iter()
        //     .map(|row| Edge { node: row })
        //     .collect();

        // println!("{:?}", asd);
        QueryResponse {
            page_info: PageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| Edge {
                    cursor: Some(row.id),
                    node: row,
                })
                .collect(),
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
