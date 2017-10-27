## TiSpark example

Usage:
1. Download or compile tispark [here](https://github.com/pingcap/tispark) and make sure it is visible in your local [maven](maven.apache.org) repository.
2. In your project pom.xml, include these dependencies:
```
<dependencies>
    <dependency>
        <groupId>com.pingcap.tispark</groupId>
        <artifactId>tispark</artifactId>
        <version>${tispark.version}</version>
    </dependency>

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-catalyst_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-hive_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-unsafe_${scala.version}</artifactId>
        <version>${spark.version}</version>
        <scope>provided</scope>
    </dependency>
</dependencies>

```  
3. Write your own application code. In this example, we create a Java file ```com.pingcap.spark.App``` like this:

```
package com.pingcap.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TiContext;

public class App {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .appName("TiSpark Application")
            .getOrCreate();

    TiContext ti = new TiContext(spark);
    ti.tidbMapDatabase("tpch", false);
    Dataset dataset = spark.sql("select count(*) from customer");
    dataset.show();
  }
}
```
We ues it to fetch all rows from table ```tpch.customer``` and print them out.

4. Run ```mvn clean package``` in your console to build ```your-application.jar```
5. ```cd``` to your spark configuration directory and add this line in your ```spark-default.conf```
```
spark.tispark.pd.addresses 127.0.0.1:2379
```
You can change your pd address according to your actual deployment environment.

6. ```cd``` to you spark home directory and run
```
./bin/spark-submit --class <main class> --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar your-application.jar
```
`--class` specifies the entry point of your application  
`--jars` specifies the TiSpark library
More information about spark-submit can be found [here](http://spark.apache.org/docs/latest/submitting-applications.html)   

In this case, we run
```
./bin/spark-submit --class com.pingcap.spark.App --jars /home/novemser/Documents/Code/PingCAP/tispark/target/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar /home/novemser/Documents/Code/Java/tisparksample/target/tispark-sample-0.1.0-SNAPSHOT.jar
```
And the results:
```
...dummy startup info ignored
+--------+
|count(1)|
+--------+
|     150|
+--------+
```

