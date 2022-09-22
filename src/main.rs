mod flash;

use axum::{
    extract::{Extension, Form, Path, Query},
    http::StatusCode,
    routing::{get, get_service, post},
    Router, Server,
};
use chrono::Utc;
use entity::{
    badges, categories, crates, crates_categories, crates_keywords, keywords, metadata,
    reserved_crate_names, sync_history, versions,
};
use flash::{post_response, PostResponse};

// use categories::Entity as Categories;

use sea_orm::{
    prelude::*,
    sea_query::{self, extension::postgres::Type},
    ConnectOptions, Database, InsertResult, QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};

use futures;
use std::{
    any::Any,
    env,
    fs::{File, self},
    future::Future,
    io::{self, Cursor, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime}, process::Command,
};

use std::sync::mpsc::channel;
use std::{
    rc::{self},
    str::FromStr,
};
use threadpool::ThreadPool;
use tower::ServiceBuilder;
use tower_cookies::{CookieManagerLayer, Cookies};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::init();

    dotenv::dotenv().ok();
    let mysql_url = env::var("DATABASE_URL").expect("DATABASE_URL is not set in .env file");
    let postgre_url = env::var("DATABASE_URL_PG").expect("DATABASE_URL_PG is not set in .env file");
    let host = env::var("HOST").expect("HOST is not set in .env file");
    let port = env::var("PORT").expect("PORT is not set in .env file");

    let server_url = format!("{}:{}", host, port);

    let mut opt = ConnectOptions::new(mysql_url.to_owned());
    opt.max_connections(512)
        .min_connections(8)
        .connect_timeout(Duration::from_secs(20))
        .idle_timeout(Duration::from_secs(8))
        .max_lifetime(Duration::from_secs(8))
        .sqlx_logging(true)
        .sqlx_logging_level(log::LevelFilter::Error);

    let mysql_conn = Database::connect(opt)
        .await
        .expect("Database connection failed");

    let pg_conn = Database::connect(postgre_url)
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

async fn download_file() -> Result<(), anyhow::Error> {
    let url = "http://static.crates.io/db-dump.tar.gz";
    // let url = "https://avatars.githubusercontent.com/u/112836202?v=4";
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data/tests/fixtures/crates");

    if !path.exists() {
        fs::create_dir_all(&path).unwrap();
    }
    path = path.join(Utc::now().timestamp().to_string() + "db-dump.tar.gz");
    let path = path.to_str().unwrap().to_string();
    let resp = reqwest::get(url).await.unwrap();
    let mut file = File::create(path).unwrap();
    file.write_all(&resp.bytes().await?).unwrap();
    Ok(())
}

fn inport_postgres() {
    let database_url = env::var("DATABASE_URL_PG").expect("DATABASE_URL_PG is not set in .env file");
    let command = format!("{}{}{}","psql ", &database_url," < import.sql");
    let output = if cfg!(target_os = "windows") {
        Command::new("cmd")
                .args(["/C", &command])
                .output()
                .expect("failed to execute process")
    } else {
        Command::new("sh")
                .arg("-c")
                .arg(command)
                .output()
                .expect("failed to execute process")
    };
    
    let output = output.stdout;
    println!("command exec result is:{}", String::from_utf8(output).unwrap() );
}

async fn sync_crates_table(
    Extension(ref data_source): Extension<DataSource>,
    mut cookies: Cookies,
) -> Result<PostResponse, (StatusCode, &'static str)> {
    download_file().await.unwrap();

    inport_postgres();

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

    // sync_data_by_date::<badges::Entity, badges::ActiveModel>(data_source).await;
    // sync_data_by_date::<categories::Entity, categories::ActiveModel>(data_source).await;
    // sync_data_by_date::<crates_categories::Entity, crates_categories::ActiveModel>(data_source).await;
    // sync_data_by_date::<crates_keywords::Entity, crates_keywords::ActiveModel>(data_source).await;
    // sync_data_by_date::<crates::Entity, crates::Model, crates::ActiveModel>(data_source,crates::Column::Id,).await;
    // sync_data_by_date::<keywords::Entity, keywords::ActiveModel>(data_source).await;
    // sync_data_by_date::<metadata::Entity, metadata::ActiveModel>(data_source).await;
    // sync_data_by_date::<reserved_crate_names::Entity, reserved_crate_names::ActiveModel>(data_source).await;
    // sync_data_by_date::<versions::Entity, versions::ActiveModel>(data_source).await;

    let data = FlashData {
        kind: "success".to_owned(),
        message: "Sync succcessfully".to_owned(),
    };
    Ok(post_response(&mut cookies, data))
}

/// generics sync table by passing entity and active_model
async fn sync_data_by_date<E, T, A>(
    data_source: &DataSource,
    order_field: E::Column,
    // create_at: E::Column,
    // sync_ops: &DataSyncOptions,
) where
    E: EntityTrait<Model = T>,
    T: ModelTrait<Entity = E>,
    A: ActiveModelTrait<Entity = E> + From<<E as EntityTrait>::Model> + Send,
{
    let models: Vec<E::Model> = E::find()
        // .filter(create_at.gte(sync_ops.sync_date))
        .order_by_asc(order_field)
        .all(&data_source.postgres)
        .await
        .expect("something wrong when fetch origin data");

    if !models.is_empty() {
        println!("pg models size: {:?}", models.len());

        // let n_workers = 4;
        // let n_jobs = 8;
        // let pool = ThreadPool::new(n_workers);
        // let (sender, receiver) = channel();

        let mut save_models: Vec<A> = Vec::new();
        // convert Model to ActiveModel
        for model in models {
            let active_model = model.into();
            save_models.push(active_model);
        }

        // It is might not possible to get only modifyed data hence
        // the created_at field will not updated when the row data modifyed
        E::delete_many()
            .exec(&data_source.mysql)
            .await
            .expect("something wrong when delete history data");

        let batch_save_size = 1000;
        let mut futures_vec = Vec::new();

        for chunk in save_models.chunks(batch_save_size) {
            // let sender = sender.clone();
            // let mut chunk_mutex = Arc::new(Mutex::new(chunk));
            // let chunk_mutex_clone = Arc::clone(&chunk_mutex);

            // pool.execute(move || {
            // let data = chunk_mutex_clone.lock().unwrap();
            let save_result =
                E::insert_many(chunk.iter().map(|x| x.clone())).exec(&data_source.mysql);
            // .await
            // .expect("something wrong when save batch");
            // sender.send(1).expect("channel will be there waiting for the pool");
            futures_vec.push(save_result);
            // });
        }
        print!("chunk size: {}", futures_vec.len());
        let res: Vec<Result<InsertResult<A>, DbErr>> = futures::future::join_all(futures_vec).await;
        for msg in res {
            match msg {
                Ok(obj) => println!("save succ"),
                Err(error) => println!("{}", error),
            }
        }
    }
}
