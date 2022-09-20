mod flash;

use axum::{
    extract::{Extension, Form, Path, Query},
    http::StatusCode,
    routing::{get, get_service, post},
    Router, Server,
};
use entity::{categories, keywords};
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
    let db_url_pg = env::var("DATABASE_URL_PG").expect("DATABASE_URL_PG is not set in .env file");
    let host = env::var("HOST").expect("HOST is not set in .env file");
    let port = env::var("PORT").expect("PORT is not set in .env file");
    let server_url = format!("{}:{}", host, port);

    let mysql_conn = Database::connect(db_url)
        .await
        .expect("Database connection failed");


    let pg_conn = Database::connect(db_url_pg)
        .await
        .expect("Database connection failed");


    let data_source = DataSource {
        postgres: pg_conn,
        mysql : mysql_conn
    };

    // let conn_vec = vec![mysql_conn, pg_conn];
    let app = Router::new()
        .route("/sync", post(sync_crates_table))
        .layer(
            ServiceBuilder::new()
                .layer(CookieManagerLayer::new())
                .layer(Extension(data_source))
        );

    let addr = SocketAddr::from_str(&server_url).unwrap();
    Server::bind(&addr).serve(app.into_make_service()).await?;

    Ok(())
}

/// define the supported db connection
#[derive(Clone)]
struct DataSource {
    pub postgres: DatabaseConnection,
    pub mysql : DatabaseConnection,
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
    Extension(ref data_source): Extension<DataSource>,
    mut cookies: Cookies,
) -> Result<PostResponse, (StatusCode, &'static str)> {

    sync_table::<categories::Entity, categories::ActiveModel>(categories::Column::Id, data_source).await;
    sync_table::<keywords::Entity, keywords::ActiveModel>(keywords::Column::Id, data_source).await;

    let data = FlashData {
        kind: "success".to_owned(),
        message: "Sync succcessfully".to_owned(),
    };
    Ok(post_response(&mut cookies, data))

}

/// generics sync table by passing entity and active_model
async fn sync_table<E, T> (id: impl ColumnTrait, data_source: &DataSource) 
    where  
    E: EntityTrait, 
    T: ActiveModelTrait<Entity = E> + From<<E as EntityTrait>::Model> {

    let models: Vec<E::Model> = E::find()
    .order_by_asc(id)
    .all(&data_source.postgres)
    .await.expect("something wrong when sync categories");

    println!("{:?}", models.get(0).as_ref());

    let mut categories: Vec<T> = Vec::new();
    // convert Model to ActiveModel
    for model in models {
        let category = model.into();
        categories.push(category);
    }
    E::insert_many(categories)
    .exec(&data_source.mysql)
    .await.expect("something wrong when sync categories");
 }
