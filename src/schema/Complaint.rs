use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct Complaint {
    pub id: Option<i32>,
    title: Option<String>,
    content: Option<String>,
    complaintstatus: Option<i32>,
    complainttype: Option<i32>,
    makerid: Option<String>,
    requestid: Option<i32>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
    userid: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct ComplaintEdge {
    cursor: Option<i32>,
    node: Complaint,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct ComplaintQueryResponse {
    page_info: ComplaintPageInfo,
    edges: Vec<ComplaintEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct ComplaintPageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_complaints<'ctx>(
    _self: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> ComplaintQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as id,Cast(Title as nvarchar) as title,Cast(Content as nvarchar(4000)) as content,ComplaintStatus as complaintstatus,ComplaintType as complainttype,Cast(MakerId as nvarchar) as  makerid,RequestId requestid,Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby,Cast(UserId as nvarchar)as userid from Complaints  where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as id,Cast(Title as nvarchar) as title,Cast(Content as nvarchar(4000)) as content,ComplaintStatus as complaintstatus,ComplaintType as complainttype,Cast(MakerId as nvarchar) as  makerid,RequestId requestid,Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby,Cast(UserId as nvarchar)as userid from Complaints"#,
            first
        ),
    };

    let row: Vec<Complaint> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("  select Count(*) as count from ComplaintReplies where Id > @p1")
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
        ComplaintQueryResponse {
            page_info: ComplaintPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| ComplaintEdge {
                    cursor: Some(row.id.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        ComplaintQueryResponse {
            page_info: ComplaintPageInfo {
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
) -> Vec<Complaint> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,Cast(Title as nvarchar) as title,Cast(Content as nVarChar(4000)) as content,ComplaintStatus as complaintstatus,ComplaintType as complainttype,Cast(MakerId as nvarchar) as  makerid,RequestId requestid,Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby,Cast(UserId as nvarchar)as userid from Complaints where ModifiedDate >='{}'"#,
                parsed_value
            );
            // get all requests the
           let asd = sqlx::query_as(&q).fetch_all(pool).await;

           asd.unwrap()
        }
        Err(e) => {
            println!("{:?}", e);
            Vec::new()
        }
    }
}