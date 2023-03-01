use super::Request;
use async_graphql::{Context, Object};
use Request::get_all_requests;
pub struct Query;
#[Object]
impl Query {
    async fn howdy(&self) -> &'static str {
        "partner"
    }

    async fn all_requests<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        first: i32,
        after: Option<i32>,
        date: Option<String>,
    ) -> Request::QueryResponse {
        get_all_requests(self, ctx, first, after, date).await
    }
}
