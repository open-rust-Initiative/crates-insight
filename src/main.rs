mod flash;

use async_std::path;
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

use flate2::read::GzDecoder;
use tar::Archive;
use sea_orm::{
    prelude::*,
    ConnectOptions, Database, InsertResult, QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};

use futures;

use std::{
    any::Any,
    env,
    fs::{File, self},
    future::Future,
    io::{self, Cursor, Write, BufReader},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration}, process::{Command, Stdio},
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
    // max_connections is properly for double size of the cpu core
    opt.max_connections(16)
        .min_connections(4)
        // wait for sea-orm new release of this config 
        // https://github.com/SeaQL/sea-orm/pull/897/commits/3e50d50822bac4565d77e78bdc589d2403e1fb9c
        // .acquire_timeout (5*60)
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

/// download and unzip sql file
async fn download_file(url: &str, file_name: &str) -> Result<String , anyhow::Error> {

    // construct base dir
    let mut base_dir = env::current_dir().unwrap().into_os_string().into_string().unwrap();
    base_dir.push_str("/temp");

    println!("download file... please wait");
    let resp = reqwest::get(url).await.unwrap();
    // let content = BufReader::new(resp);

    let mut zip_path = base_dir.clone();
    zip_path.push_str(file_name);
    let mut file = File::create(&zip_path).unwrap();
    file.write_all(&resp.bytes().await?).unwrap();

    println!("download success, unzip file... please wait");

    let file = File::open(&zip_path)?;
    let gz = GzDecoder::new(file);
    let mut archive = Archive::new(gz);

    let mut path_to_script = base_dir.clone();
    // search import.sql file in archive and get relative path
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_owned().to_path_buf();
        // unpack each entry in to base_dir
        entry.unpack_in(&base_dir)?;
        println!("{}", &path.display());
        let path = path.into_os_string().into_string().unwrap();
        if path.contains("import.sql") {
            path_to_script.push('/');
            path_to_script.push_str(path.strip_suffix("/import.sql").unwrap())
        }
    }
    Ok(path_to_script)
}

/// get path_to_script from zip file and execute the import.sql file by psql
fn import_postgres(path_to_script: &str, import_sql_path: &str) {
    println!("path_to_script: {}", path_to_script);
    //TODO config password in env file

    let mut cmd = 
        Command::new("psql")
                .args(["-v", &format!("{}{}","scriptdir=", path_to_script)])
                .args(["-U", "postgres"])
                .args(["-d", "postgres"])
                .args(["-f", import_sql_path])
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
    println!("{:?}", cmd.wait());
    // remove unzip file
    fs::remove_dir_all(path_to_script).unwrap();
}

async fn sync_crates_table(
    Extension(ref data_source): Extension<DataSource>,
    mut cookies: Cookies,
) -> Result<PostResponse, (StatusCode, &'static str)> {

    // remote dump file url
    let url = "http://static.crates.io/db-dump.tar.gz";


    let base_dir = env::current_dir().unwrap().into_os_string().into_string().unwrap();

    //local data directory
    let path_to_script = download_file(url,"/db-dump.tar.gz").await.unwrap();

    import_postgres(&path_to_script, &format!("{}{}",base_dir, "/temp/import.sql"));

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
    // sync_data_by_date::<crates::Entity, crates::Model, crates::ActiveModel>(data_source,crates::Column::Id).await;
    // sync_data_by_date::<keywords::Entity, keywords::ActiveModel>(data_source).await;
    // sync_data_by_date::<metadata::Entity, metadata::ActiveModel>(data_source).await;
    // sync_data_by_date::<reserved_crate_names::Entity, reserved_crate_names::ActiveModel>(data_source).await;
    sync_data_by_date::<versions::Entity, versions::ActiveModel>(data_source, versions::Column::Id).await;

    let data = FlashData {
        kind: "success".to_owned(),
        message: "Sync succcessfully".to_owned(),
    };
    Ok(post_response(&mut cookies, data))
}

/// generics sync table by passing entity and active_model
async fn sync_data_by_date<E, A>(
    data_source: &DataSource,
    order_field: E::Column,
    // create_at: E::Column,
    // sync_ops: &DataSyncOptions,
) where
    E: EntityTrait,
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
        // so it need to clear all history data
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
            // sender.send(1).expect("channel will be there waiting for the pool");
            futures_vec.push(save_result);
            // });
        }

        let res: Vec<Result<InsertResult<A>, DbErr>> = futures::future::join_all(futures_vec).await;
        for msg in res {
            match msg {
                Ok(obj) => println!("save succ"),
                Err(error) => println!("{}", error),
            }
        }
    }
}


#[cfg(test)]
pub mod tests {
    use std::{env, fs, path::PathBuf};
    use crate::{import_postgres, download_file};

    #[tokio::test]
    async fn test_download_and_unpack() {
        let path_to_script = download_file("https://github.com/open-rust-initiative/crates-insight/raw/main/tests/db-dump.tar.gz", "test.tar.gz").await.unwrap();
        println!("path_to_script: {}", path_to_script);
        assert!(path_to_script.contains("2022-09-25-020017"));
    }

    // #[tokio::test]
    // async fn test_import_postgres() {
    //     let mut path_to_script = env::current_dir().unwrap().into_os_string().into_string().unwrap();
    //     let mut import_sql_path = path_to_script.clone();
    //     path_to_script.push_str("/temp/2022-09-25-020017");
    //     import_sql_path.push_str("/temp/2022-09-25-020017/import.sql");
    //     import_postgres(&path_to_script, &import_sql_path);
    // }
}