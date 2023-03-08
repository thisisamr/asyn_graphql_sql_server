use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct UserProfile {
    pub id: Option<i32>,
    telephonenumber: Option<String>,
    pub userid: Option<String>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    haswhatsapp: Option<bool>,
    phonenumbertype: Option<i32>,
    description: Option<String>,
    #[graphql(name = "sync_status")]
    sync_status: Option<i32>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct UserProfileEdge {
    cursor: Option<i32>,
    node: UserProfile,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct UserProfileQueryResponse {
    page_info: UserProfilePageInfo,
    edges: Vec<UserProfileEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct UserProfilePageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_user_profiles<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> UserProfileQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as id,cast(TelephoneNumber as nvarchar)  as telephonenumber, UserId as userid, Cast(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar) as modifieddate, cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,HasWhatsApp as haswhatsapp, PhoneNumberType as phonenumbertype, Cast(Description as nvarchar)as description,SyncStatus as sync_status  from UserProfiles where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as id,cast(TelephoneNumber as nvarchar) as telephonenumber, UserId as userid, Cast(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar) as modifieddate, cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,HasWhatsApp as haswhatsapp, PhoneNumberType as phonenumbertype, Cast(Description as nvarchar)as description,SyncStatus as sync_status   from UserProfiles "#,
            first
        ),
    };

    let row: Vec<UserProfile> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from UserProfiles where Id > @p1")
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
        UserProfileQueryResponse {
            page_info: UserProfilePageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| UserProfileEdge {
                    cursor: Some(row.id.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        UserProfileQueryResponse {
            page_info: UserProfilePageInfo {
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
) -> Vec<UserProfile> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,cast(TelephoneNumber as nvarchar) as telephonenumber, UserId as userid, Cast(AddedDate as varchar)as addeddate,CAST(ModifiedDate as varchar) as modifieddate, cast(Createdby as nvarchar) as createdby,cast(UpdatedBy as nvarchar) as updatedby,HasWhatsApp as haswhatsapp, PhoneNumberType as phonenumbertype, Cast(Description as nvarchar)as description,SyncStatus as sync_status   from UserProfiles where ModifiedDate >='{}'"#,
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