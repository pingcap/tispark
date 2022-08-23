# Start a local TiDB cluster for testing
TiSpark needs some metadata stored in TiDB, like user authority. 
And TiSpark will write data to TiKV and read data from it.
So we need to install TiDB and TiKV. It's really easy to deploy a local test cluster for development using TiUP.
We give a simple guide about how to use it in the following.
You can find more details about TiUP [here](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb).
1. Download TiUP

   `curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh`
   
   If the following message is displayed, you have installed TiUP successfully:
   ```
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
   
   `source ${your_shell_profile}`
3. Download TiDB config file

   The test data of TiSpark is very complicated, so that the default config of TiDB is not enough.
   You need to get a copy of TiDB config file from [here](https://github.com/pingcap/tidb/blob/master/config/config.toml.example),
   rename it to `config.toml` and set `index-limit` to 512 if you want to pass the tispark test
  
4. Start the cluster in the current session
   
   `tiup playground --db.config config.toml`

   This command will download required files and  start a TiDB cluster of the latest version with 1 TiDB instance, 1 TiKV instance, 1 PD instance, and 1 TiFlash instance. 