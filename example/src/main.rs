use std::{env, path::PathBuf, sync::Arc, process::exit};

use configfs::{Configuration, ConfigFS, Event, Data, Result, async_trait};
use tokio::{sync::{Mutex}};

struct JsonData;

#[async_trait]
impl Data for JsonData {
    async fn fetch(&mut self, ino: u64) -> Result<Vec<u8>> {
        todo!()
    }
    async fn update(&mut self, ino: u64, data: Vec<u8>) -> Result<()> {
        todo!()
    }
}


#[tokio::main]
async fn main() {
    let mut args = env::args_os().skip(1);
    let Some(mnt) = args.next().map(|m| PathBuf::from(m)) else {
        print!("Mount path was not specified");
        exit(1);
    };
    let Some(json) = args.next().map(|m| PathBuf::from(m)) else {
        print!("JSON path was not specified");
        exit(1);
    };


    let config = Configuration::new();
    let (mut events, mount_handle) = ConfigFS::mount(
        "test", 
        &mnt.to_string_lossy().to_string(), 
        config.clone()
    ).await.unwrap(); 
    let json_data = Arc::new(Mutex::new(JsonData));    

    let event_handle = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                Event::Mkdir { parent, name, sender } => {
                    let mut config = config.write().await;
                    match config.create_group(parent, &name) {
                        Ok(ino) => {sender.send(Some(ino)).await.unwrap();},
                        Err(_) => {sender.send(None).await.unwrap();},
                    }
                },
                Event::Mk { parent, name, sender } => {
                    let mut config = config.write().await;
                    match config.create_file(
                        parent, 
                        &name, 
                        json_data.clone()
                    ) {
                        Ok(ino) => {sender.send(Some(ino)).await.unwrap();},
                        Err(_) => {sender.send(None).await.unwrap();},
                    }
                },
                Event::Rm { parent, name, sender } => {
                    let mut config = config.write().await;
                    match config.remove(parent, &name) {
                        Ok(_) => {sender.send(true).await.unwrap();},
                        Err(_) => {sender.send(false).await.unwrap();},
                    }
                },
                Event::Mv { parent, new_parent, name, new_name, sender } => {
                    let mut config = config.write().await;
                    match config.mv(parent, new_parent, &name, &new_name) {
                        Ok(_) => {sender.send(true).await.unwrap();},
                        Err(_) => {sender.send(false).await.unwrap();},
                    }
                },
                Event::Rename { parent, name, new_name, sender } => {
                    let mut config = config.write().await;
                    match config.rename(parent, &name, &new_name) {
                        Ok(_) => {sender.send(true).await.unwrap();},
                        Err(_) => {sender.send(false).await.unwrap();},
                    }
                },
            }
        }
    });
    
    let (event_error, mount_error) = tokio::join!(event_handle, mount_handle);
    if let Err(e) = event_error {
        println!("{}", e);
    }
    if let Err(e) = mount_error {
        println!("{}", e);
    }
}
