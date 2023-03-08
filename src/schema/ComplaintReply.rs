use super::{Query, Request::RecordCount};
use async_graphql::{Context, SimpleObject};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Mssql, Pool};
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct ComplaintReply {
    id: Option<i32>,
    complaintid: Option<i32>,
    content: Option<String>,
    makerid: Option<String>,
    addeddate: Option<String>,
    modifieddate: Option<String>,
    createdby: Option<String>,
    updatedby: Option<String>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize, Clone)]
pub struct ComplaintReplyEdge {
    cursor: Option<i32>,
    node: ComplaintReply,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct ComplaintReplyQueryResponse {
    page_info: ComplaintReplyPageInfo,
    edges: Vec<ComplaintReplyEdge>,
}
#[derive(FromRow, SimpleObject, Debug, Deserialize, Serialize)]
pub struct ComplaintReplyPageInfo {
    endCursor: i32,
    hasNextPage: bool,
}

pub async fn get_all_complaint_replys<'ctx>(
    _self: &Query::Query,
    ctx: &Context<'ctx>,
    first: i32,
    after: Option<i32>,
) -> ComplaintReplyQueryResponse {
    let mut hasNext: bool = false;
    let pool = ctx.data::<Pool<Mssql>>().unwrap();

    let q = match after {
        Some(cursor_id) => format!(
            r#"select Top({}) Id as id,ComplaintId as complaintid,Cast(Content as nvarchar(4000)) as content,Cast( MakerId as nvarchar)as makerid, Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby from ComplaintReplies where Id > {}"#,
            first, cursor_id
        ),

        None => format!(
            r#"select Top({}) Id as id,ComplaintId as complaintid,Cast(Content as nvarchar(4000)) as content,Cast( MakerId as nvarchar)as makerid, Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby from ComplaintReplies"#,
            first
        ),
    };

    let row: Vec<ComplaintReply> = sqlx::query_as(&q).fetch_all(pool).await.unwrap();
    if row.len() != 0 {
        let curs = row[row.len() - 1].id.unwrap();
        let secondquery: Result<RecordCount, sqlx::Error> =
            sqlx::query_as("select Count(*) as count from ComplaintReplies where Id > @p1")
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
        ComplaintReplyQueryResponse {
            page_info: ComplaintReplyPageInfo {
                endCursor: curs,
                hasNextPage: hasNext,
            },
            edges: row
                .into_iter()
                .map(|row| ComplaintReplyEdge {
                    cursor: Some(row.id.unwrap()),
                    node: row,
                })
                .collect(),
        }
    } else {
        ComplaintReplyQueryResponse {
            page_info: ComplaintReplyPageInfo {
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
) -> Vec<ComplaintReply> {
    let pool = ctx.data::<Pool<Mssql>>().unwrap();
    let parsed = NaiveDateTime::parse_from_str(last_sync_timestamp.trim(), "%Y-%m-%d %H:%M:%S%.f");
    
    match parsed {
        Ok(parsed_value) => {
            let q = format!(
                r#"select Id as id,ComplaintId as complaintid,Cast(Content as nvarchar(4000)) as content,Cast( MakerId as nvarchar)as makerid, Cast(AddedDate as varchar)as addeddate,Cast(ModifiedDate as varchar)as modifieddate,Cast(Createdby as nvarchar)as createdby,Cast(UpdatedBy as nvarchar)as updatedby from ComplaintReplies where ModifiedDate >='{}'"#,
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