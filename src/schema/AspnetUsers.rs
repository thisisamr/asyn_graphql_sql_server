use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct AspnetUsers {
    id: String,
    arabicfullname: Option<String>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    makerid: Option<String>,
    dateofbirth: Option<String>,
    firstlogin: Option<bool>,
    addressid: Option<i32>,
    username: Option<String>,
    normalizedusername: Option<String>,
    email: Option<String>,
    normalizedemail: Option<String>,
    emailconfirmed: Option<bool>,
    passwordhash: Option<String>,
    securitystamp: Option<String>,
    concurrencystamp: Option<String>,
    phonenumber: Option<String>,
    phonenumberconfirmed: Option<bool>,
    twofactorenabled: Option<bool>,
    lockoutendl: Option<String>,
    lockoutenabled: Option<bool>,
    accessfailedcount: Option<i32>,
    #[graphql(name = "sync_status")]
    sync_status: Option<i32>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct UserEdge {
    cursor: Option<String>,
    node: AspnetUsers,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct UserQueryResponse {
    page_info: UserPageInfo,
    edges: Vec<UserEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct UserPageInfo {
    endCursor: String,
    hasNextPage: bool,
}

pub async fn get_all_users<'ctx>(
    _slef: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<String>,
) -> UserQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select TOP({}) Id as id,ArabicFullName as arabicfullname,Cast(AddedDate as varchar) as addeddate,FirstLogIn as firstlogin,Cast(ModifiedDate as varchar) as modifieddate,MakerId as makerid  ,Cast(Cast(DateOfBirth as date) as varchar) as dateofbirth,AddressId as addressid,UserName as username, NormalizedUserName as normalizedusername,Email as email,NormalizedEmail as normalizedemail ,EmailConfirmed as emailconfirmed, Cast(PasswordHash as varchar) as passwordhash,CAST(SecurityStamp AS varchar) as securitystamp,CAST(ConcurrencyStamp AS varchar) as concurrencystamp,CAST(PhoneNumber AS varchar) as phonenumber,PhoneNumberConfirmed as phonenumberconfirmed , TwoFactorEnabled as twofactorenabled,LockoutEnabled as lockoutenabled,LockoutEnd as lockoutendl,AccessFailedCount as accessfailedcount,SyncStatus as sync_status  from AspNetUsers where Id > '{}'"#,
            first, cursor_id
        ),

        None => format!(
            r#"select TOP({}) Id as id,ArabicFullName as arabicfullname,Cast(AddedDate as varchar) as addeddate,FirstLogIn as firstlogin,Cast(ModifiedDate as varchar) as modifieddate,MakerId as makerid  ,Cast(Cast(DateOfBirth as date) as varchar ) as dateofbirth,AddressId as addressid,UserName as username, NormalizedUserName as normalizedusername,Email as email,NormalizedEmail as normalizedemail ,EmailConfirmed as emailconfirmed, Cast(PasswordHash as varchar) as passwordhash,CAST(SecurityStamp AS varchar) as securitystamp,CAST(ConcurrencyStamp AS varchar) as concurrencystamp,CAST(PhoneNumber AS varchar) as phonenumber,PhoneNumberConfirmed as phonenumberconfirmed , TwoFactorEnabled as twofactorenabled,LockoutEnabled as lockoutenabled,LockoutEnd as lockoutendl,AccessFailedCount as accessfailedcount,SyncStatus as sync_status  from AspNetUsers"#,
            first
        ),
    };

    let row: Vec<AspnetUsers> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();

    if row.len() != 0 {
        let curs = row[row.len() - 1].id.clone();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from AspNetUsers where Id > '@p1'")
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
        UserQueryResponse {
            page_info: UserPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| UserEdge {
                    cursor: Some(row.id.clone()),
                    node: row,
                })
                .collect(),
        }
    } else {
        UserQueryResponse {
            page_info: UserPageInfo {
                endCursor: String::new(),
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
) -> Vec<AspnetUsers> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,ArabicFullName as arabicfullname,Cast(AddedDate as varchar) as addeddate,FirstLogIn as firstlogin,Cast(ModifiedDate as varchar) as modifieddate,MakerId as makerid  ,Cast(Cast(DateOfBirth as date) as varchar ) as dateofbirth,AddressId as addressid,UserName as username, NormalizedUserName as normalizedusername,Email as email,NormalizedEmail as normalizedemail ,EmailConfirmed as emailconfirmed, Cast(PasswordHash as varchar) as passwordhash,CAST(SecurityStamp AS varchar) as securitystamp,CAST(ConcurrencyStamp AS varchar) as concurrencystamp,CAST(PhoneNumber AS varchar) as phonenumber,PhoneNumberConfirmed as phonenumberconfirmed , TwoFactorEnabled as twofactorenabled,LockoutEnabled as lockoutenabled,LockoutEnd as lockoutendl,AccessFailedCount as accessfailedcount,SyncStatus as sync_status  from AspNetUsers where ModifiedDate >='{}'"#,
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