# ConfigFS: Everything Should Be An File

ConfigFS is a library for creating filesystem's that allow users to view and modify structured data and settings. It is inspired by another filesystem of the same name, ConfigFS, a ram based filesystem on linux that allows users to configure kernel objects.

ConfigFS relies on fuse3. It can be installed via your favorite package manager.

You can view the docs [here](https://ellabellla.github.io/configfs/configfs/)

## How Does It Work?
```rust
struct FavNumberConfig {
    fav_number: i64,
    output: String,
    modified: bool,
}

impl FavNumberConfig {
    pub fn new(fav_number: i64, output: String) -> Configuration {
        Configuration::Basic(Arc::new(RwLock::new(FavNumberConfig{fav_number, output, modified: false})))
    }
}

#[async_trait]
impl BasicConfigHook for FavNumberConfig {
    async fn fetch(&mut self) -> Result<Vec<u8>> {
        Ok(format!("{}", self.fav_number).as_bytes().to_vec())
    }

    async fn size(&mut self) -> Result<u64> {
        Ok(format!("{}", self.fav_number).as_bytes().len() as u64)
    }

    async fn update(&mut self, data: Vec<u8>) -> Result<()> {
        let input = String::from_utf8(data).map_err(|_| Errno::from(libc::EIO))?;
        let number = i64::from_str_radix(&input, 10).map_err(|_| Errno::from(libc::EIO))?;

        self.fav_number = number;
        println!("My new favourite number is {}", number);
        self.modified = true;
        Ok(())
    }
    
    async fn tick(&mut self) {
        if !self.modified {
            return
        }

        self.modified = false;
        let mut Ok(file) = fs::File::create(&self.output).await else {
            return
        };
        file.write_all(format!("{}", self.fav_number).as_bytes()).await.ok();
    }

    fn tick_interval(&self) -> Duration {
        Duration::from_secs(1)
    }
}
```
First you create your config interface by implementing the config type's respective trait. Basic configs function as files. Complex configs function as directories.


```rust 
let mount = Mount::new();
{
    let mut mnt = mount.write().await;
    mnt.mount("/", FavNumberConfig::new(42));
}
```
Then you define how your configs should be mounted into the config filesystem. This allows you to mount multiple config interfaces into a single filesystem.


```rust
FS::mount("Fav Number FS", "/mnt/fav_number", mount)
    .await
    .unwrap()
    .await
    .unwrap()
    .unwrap()
```
Finally you mount your config filesystem.


## License
This software is provided under the MIT license. Click [here](./LICENSE) to view.
