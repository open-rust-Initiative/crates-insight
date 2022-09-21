mod flash;

use axum::{
    extract::{Extension, Form, Path, Query},
    http::StatusCode,
    routing::{get, get_service, post},
    Router, Server,
};
use entity::{
    badges, categories, crates, crates_categories, crates_keywords, keywords, metadata,
    reserved_crate_names, sync_history, versions,
};
use flash::{post_response, PostResponse};

// use categories::Entity as Categories;

use sea_orm::{
    prelude::*,
    sea_query::{self, extension::postgres::Type},
    Database, QuerySelect,
};
use serde::{Deserialize, Serialize};


use std::{env, net::SocketAddr, any::Any};
use std::{
    rc::{self},
    str::FromStr,
};
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
        mysql: mysql_conn,
    };

    // let conn_vec = vec![mysql_conn, pg_conn];
    let app = Router::new().route("/sync", post(sync_crates_table)).layer(
        ServiceBuilder::new()
            .layer(CookieManagerLayer::new())
            .layer(Extension(data_source)),
    );

    let addr = SocketAddr::from_str(&server_url).unwrap();
    Server::bind(&addr).serve(app.into_make_service()).await?;

    Ok(())
}

/// define the supported db connection
#[derive(Clone)]
struct DataSource {
    pub postgres: DatabaseConnection,
    pub mysql: DatabaseConnection,
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

pub struct DataSyncOptions {
    pub sync_date: Date,
}

async fn sync_crates_table(
    Extension(ref data_source): Extension<DataSource>,
    mut cookies: Cookies,
) -> Result<PostResponse, (StatusCode, &'static str)> {

    let result: Vec<sync_history::Model> = sync_history::Entity::find()
        .filter(sync_history::Column::Success.eq(0))
        .limit(1)
        .all(&data_source.mysql)
        .await
        .expect("s");

    let sync_date = match result.get(0) {
        Some(x) => {
            println!("last sync date was: {}", x.date);
            x.date
        }
        None => todo!(),
    };
    let sync_ops = DataSyncOptions {
        sync_date: sync_date,
    };
    sync_data_by_date::<badges::Entity, badges::ActiveModel>(data_source).await;
    sync_data_by_date::<categories::Entity, categories::ActiveModel>(data_source).await;
    sync_data_by_date::<crates_categories::Entity, crates_categories::ActiveModel>(data_source).await;
    sync_data_by_date::<crates_keywords::Entity, crates_keywords::ActiveModel>(data_source).await;
    sync_data_by_date::<crates::Entity, crates::ActiveModel>(data_source).await;
    sync_data_by_date::<keywords::Entity, keywords::ActiveModel>(data_source).await;
    sync_data_by_date::<metadata::Entity, metadata::ActiveModel>(data_source).await;
    sync_data_by_date::<reserved_crate_names::Entity, reserved_crate_names::ActiveModel>(data_source).await;
    sync_data_by_date::<versions::Entity, versions::ActiveModel>(data_source).await;

    let data = FlashData {
        kind: "success".to_owned(),
        message: "Sync succcessfully".to_owned(),
    };
    Ok(post_response(&mut cookies, data))
}

/// generics sync table by passing entity and active_model
async fn sync_data_by_date<E, T>(
    data_source: &DataSource,
    // create_at: E::Column,
    // id: E::Column,
    // sync_ops: &DataSyncOptions,
) where
    E: EntityTrait,
    T: ActiveModelTrait<Entity = E> + From<<E as EntityTrait>::Model>,
{
    let models: Vec<E::Model> = E::find()
        // .filter(create_at.gte(sync_ops.sync_date))
        .all(&data_source.postgres)
        .await
        .expect("something wrong when fetch origin data");

    if !models.is_empty() {
        println!("pg models size: {:?}", models.len());

        // let model_ids: Vec<i32> = Vec::new();
        let mut save_models: Vec<T> = Vec::new();
        // convert Model to ActiveModel
        for model in models {
            let active_model = model.into();
            save_models.push(active_model);
            // model_ids.push(model.id);
        }

        E::delete_many()
            .exec(&data_source.mysql)
            .await
            .expect("something wrong when delete history data");

        let batch_save_size = 500;
        for chunk in save_models.chunks(batch_save_size) {
            E::insert_many(chunk.iter().map(|x| x.clone()))
                // .on_conflict(
                //     sea_query::OnConflict::column(id)
                //     .update_column(name)
                //     .to_owned()
                // )
                .exec(&data_source.mysql)
                .await
                .expect("something wrong when save batch");
        }
    }
}
