use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct RequestPriceDifference {
    id: Option<i32>,
    price: Option<f64>,
    requestid: Option<i32>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    orderstatus: Option<i32>,
    areadifference: Option<f64>,
    description: Option<String>,
    requestareadifferencestatus: Option<i32>,
    subunitareadifference: Option<f64>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct RequestPriceDifferenceEdge {
    cursor: Option<i32>,
    node: RequestPriceDifference,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct RequestPriceDifferenceQueryResponse {
    page_info: RequestPriceDifferencePageInfo,
    edges: Vec<RequestPriceDifferenceEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct RequestPriceDifferencePageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_request_price_differences<'ctx>(
    _self: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> RequestPriceDifferenceQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as id,Price as price,RequestId as requestid,cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,Cast(UpdatedBy as nvarchar) as updatedby,OrderStatus as orderstatus,AreaDifference as areadifference,Cast(Description as nvarchar)as description,RequestAreaDifferenceStatus as requestareadifferencestatus, SubUnitAreaDifference as subunitareadifference from RequestPriceDifferences where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as id,Price as price,RequestId as requestid,cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,Cast(UpdatedBy as nvarchar) as updatedby,OrderStatus as orderstatus,AreaDifference as areadifference,Cast(Description as nvarchar)as description,RequestAreaDifferenceStatus as requestareadifferencestatus, SubUnitAreaDifference as subunitareadifference from RequestPriceDifferences"#,
            first
        ),
    };

    let row: Vec<RequestPriceDifference> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from UserAddresses where Id > @p1")
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
        RequestPriceDifferenceQueryResponse {
            page_info: RequestPriceDifferencePageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| RequestPriceDifferenceEdge {
                    cursor: Some(row.id.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        RequestPriceDifferenceQueryResponse {
            page_info: RequestPriceDifferencePageInfo {
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
) -> Vec<RequestPriceDifference> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,Price as price,RequestId as requestid,cast(AddedDate as varchar) as addeddate, Cast(ModifiedDate as varchar) as modifieddate,Cast(Createdby as nvarchar) as createdby,Cast(UpdatedBy as nvarchar) as updatedby,OrderStatus as orderstatus,AreaDifference as areadifference,Cast(Description as nvarchar)as description,RequestAreaDifferenceStatus as requestareadifferencestatus, SubUnitAreaDifference as subunitareadifference from RequestPriceDifferences where ModifiedDate >='{}'"#,
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