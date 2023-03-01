mod schema;

use actix_web::{guard, web, web::Data, App, HttpResponse, HttpServer, Result};
use async_graphql::{
    http::{playground_source, GraphQLPlaygroundConfig},
    *,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use sqlx::mssql::MssqlPoolOptions;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let pool = MssqlPoolOptions::new()
        .connect("mssql://sa:apollo@6337497@localhost/REMVC3")
        .await;

    let schema = Schema::build(schema::Query::Query, EmptyMutation, EmptySubscription)
        .data(pool.unwrap())
        .finish();

    println!("GraphiQL IDE: http://localhost:8000");

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(schema.clone()))
            .service(web::resource("/").guard(guard::Post()).to(index))
            .service(web::resource("/").guard(guard::Get()).to(index_graphiql))
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}

async fn index(
    schema: web::Data<Schema<schema::Query::Query, EmptyMutation, EmptySubscription>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}
// to use GraphiQL instead
//.body(GraphiQLSource::build().endpoint("/").finish()))

async fn index_graphiql() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(playground_source(GraphQLPlaygroundConfig::new(
            "http://localhost:8000",
        ))))
}
