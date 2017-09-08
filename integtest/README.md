# Introduction 
This is integration test for `TiSpark` including `TiKV-java-client`. Correctness of both project is assured by this integration test. Any new coming pull request has to pass this test in order to merge into master. 

# How to use this intergration test framework?
Before you run this intergration test, it requires a `TiDB` cluster present. When you do not have one, do not expect this integtation work. 
If you do have a `TiDB` cluster present, you can move forward. There is still one important step left for runing this test properly. It is generate and load `tpch` data to your `TiDB` cluster. 
You can `cd tpch` and execute `bash genandloadalldata.sh`. After that, you are good to go. Integration test can be triggered by executing `bash test.sh`.
