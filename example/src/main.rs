use std::{env::{self}, time::Duration};

use configfs::{Mount, FS, basic::json::JsonConfig, Configuration};
use tokio::fs;

#[tokio::main]
pub async fn main() {
    let mut args = env::args().skip(1);

    let Some(mnt) = args.next() else {
        println!("a mount point must be specified");
        return;
    };

    let json_config: Configuration = if let Some(json) = args.next() {
        let Ok(data) = fs::read_to_string(&json).await  else {
            println!("json file could not be loaded");
            return;
        };

        let mut json_config = match serde_json::from_str::<JsonConfig>(&data) {
            Ok(json_config) => json_config,
            Err(e) =>  {
                println!("json couldn't be parsed, {}", e);
                return;
            }
        };
        json_config.set_output(Some((&json, Duration::from_secs(1))));
        json_config.into()
    } else {
        JsonConfig::new_obj(None)
    };

    let mount = Mount::new();
    {
        let mut mnt = mount.write().await;
        mnt.mount("/", json_config);
    }
    FS::mount("test", &mnt, mount)
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap()
} 