mod flash;

use axum::{
    extract::{Extension, Form, Path, Query},
    http::StatusCode,
    routing::{get, get_service, post},
    Router, Server,
};
use entity::{categories};
use flash::{post_response, PostResponse};

use categories::Entity as Categories;

use sea_orm::{prelude::*, Database, QueryOrder, Set};
use serde::{Deserialize, Serialize};

use std::str::FromStr;
use std::{env, net::SocketAddr};
use tera::Tera;
use tower::ServiceBuilder;
use tower_cookies::{CookieManagerLayer, Cookies};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::init();

    dotenv::dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    let host = env::var("HOST").expect("HOST is not set in .env file");
    let port = env::var("PORT").expect("PORT is not set in .env file");
    let server_url = format!("{}:{}", host, port);

    let mysql_conn = Database::connect(db_url)
        .await
        .expect("Database connection failed");


    let pg_conn = Database::connect("postgres://postgres:postgres@localhost/axum_example")
        .await
        .expect("Database connection failed");



    let conn_vec = vec![mysql_conn, pg_conn];
    let app = Router::new()
        .route("/sync", post(sync_crates_table))
        .layer(
            ServiceBuilder::new()
                .layer(CookieManagerLayer::new())
                .layer(Extension(conn_vec))
        );

    let addr = SocketAddr::from_str(&server_url).unwrap();
    Server::bind(&addr).serve(app.into_make_service()).await?;

    Ok(())
}

#[derive(Deserialize)]
struct Params {
    page: Option<u64>,
    posts_per_page: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct FlashData {
    kind: String,
    message: String,
}

async fn sync_crates_table (
    Extension(ref conn_vec): Extension<Vec<DatabaseConnection>>,
    Query(params): Query<Params>,
    mut cookies: Cookies,
) -> Result<PostResponse, (StatusCode, &'static str)> {


    Categories::find()
    // .filter(cake::Column::Name.contains("chocolate"))
    .order_by_asc(categories::Column::Id)
    .all(&conn_vec[0])
    .await.expect("msg");

    let data = FlashData {
        kind: "success".to_owned(),
        message: "Sync succcessfully".to_owned(),
    };
    Ok(post_response(&mut cookies, data))

}
