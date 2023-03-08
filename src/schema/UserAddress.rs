use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct UserAddress {
    id: Option<i32>,
    description: Option<String>,
    districtid: Option<i32>,
    userprofileid: Option<i32>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    regionid: Option<i32>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct UserAddressEdge {
    cursor: Option<i32>,
    node: UserAddress,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct UserAddressQueryResponse {
    page_info: UserAddressPageInfo,
    edges: Vec<UserAddressEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct UserAddressPageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_user_addresses<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> UserAddressQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as  id, Cast(Description as nvarchar) as description,DistrictId as districtid,UserProfileId as userprofileid,Cast(AddedDate as varchar) as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby ,Cast(UpdatedBy as nvarchar) as updatedby ,RegionId as regionid from UserAddresses where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as  id, Cast(Description as nvarchar) as description,DistrictId as districtid,UserProfileId as userprofileid,Cast(AddedDate as varchar) as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby ,Cast(UpdatedBy as nvarchar) as updatedby ,RegionId as regionid from UserAddresses"#,
            first
        ),
    };

    let row: Vec<UserAddress> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
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
        UserAddressQueryResponse {
            page_info: UserAddressPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| UserAddressEdge {
                    cursor: Some(row.id.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        UserAddressQueryResponse {
            page_info: UserAddressPageInfo {
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
) -> Vec<UserAddress> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as  id, Cast(Description as nvarchar) as description,DistrictId as districtid,UserProfileId as userprofileid,Cast(AddedDate as varchar) as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar) as createdby ,Cast(UpdatedBy as nvarchar) as updatedby ,RegionId as regionid from UserAddresses where ModifiedDate >='{}'"#,
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