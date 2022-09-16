# Start a local TiDB cluster for testing

TiSpark needs some metadata stored in TiDB, like user authority. And TiSpark will write data to TiKV and read data from it.

So We need to deploy the TiDB cluster which allows us to run tests across different platforms.

- You can use TiUP to set up the TiDB cluster.

- You can also use docker-compose

## Use TiUP

You can find more details about TiUP [here](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb).

1. Download TiUP: If the following message is displayed, you have installed TiUP successfully:
   ```
   $ `curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh`
   Successfully set mirror to https://tiup-mirrors.pingcap.com
   Detected shell: zsh
   Shell profile:  /Users/user/.zshrc
   /Users/user/.zshrc has been modified to add tiup to PATH
   open a new terminal or source /Users/user/.zshrc to use it
   Installed path: /Users/user/.tiup/bin/tiup
   ===============================================
   Have a try:     tiup playground
   ===============================================
   ```
2. Declare the global environment variable
   ```
   source ${your_shell_profile}
   ```
3. Config

   The default config of TiDB may not enough, you need to change some configs of TiDB,TiKV and PD for test.
   You can get a copy of TiDB config file from [here](https://github.com/pingcap/tidb/blob/master/config/config.toml.example) with the default value.
   rename it to `config.toml` and at least set `index-limit` to 512 if you want to pass the tispark test.
   
   About other configs you may need to change, see [here](../../../config) for more details.
  
5. Start the cluster locally
   
   ```
   tiup playground --db.config config.toml
   ```

   This command will download required files and start a TiDB cluster of the latest version with 1 TiDB instance, 1 TiKV instance, 1 PD instance, and 1 TiFlash instance.

## Use docker-compose

> Make sure you have a docker service

The yaml of docker-compose is under the home directory of TiSpark.You can choose the version you need.
- To deploy the TiDB cluster, launch the TiDB cluster service via docker-compose up. 
- To shut down the entire TiDB cluster service, use docker-compose down. 

All the data is stored in the data directory at the root of this project. You can change it as you like.

