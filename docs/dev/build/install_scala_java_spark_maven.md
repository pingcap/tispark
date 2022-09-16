# Install Scala && Java && Maven && Spark
TiSpark is connector for Spark that enables you to use TiDB as the data source of Apache Spark, similar to other data sources. 
Before developing, you need to install the following software.
## Install Scala
We use Scala to develop TiSpark, so we need to install Scala.

TiSpark3.x are using Scala 2.12 . You can find out how to install Scala at [here](https://www.scala-lang.org/download/2.12.16.html).

## Install Java 8
Our client for TiKV is written in Java, so we need to install Java.

We are using Java 8 now. You can get Java 8 at [here](https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html). 

If you are using MacOS for M1, [Zulu JDK](https://www.azul.com/downloads/?version=java-8-lts&os=macos&architecture=arm-64-bit&package=jdk) may be a choice for you.

## Install Maven
   We use Maven to do project management. 
   
   You can download the most recently released [Maven](https://maven.apache.org/download.cgi) 
   or history version from [here](https://archive.apache.org/dist/maven/maven-3/).
   Please follow the [official installation guide](https://maven.apache.org/install.html) to install it.
   Just make sure it supports JDK 8.
## Install Spark (Not necessary for build and test)

> Spark is not needed if you just want to run tests when developing,

You need to install spark once you want to use TiSpark with jar package. see [Getting-Started](https://github.com/pingcap/tispark/wiki/Getting-Started) on how to use it.
 
See [versions info](https://github.com/pingcap/tispark/wiki/Getting-TiSpark#getting-tispark-jar) here to determine which spark version you need.

You can get Spark at [Downloads | Apache Spark](https://spark.apache.org/downloads.html).

Installation of spark is out of the scope of this document. If you have any questions, please turn to [Spark](https://spark.apache.org/) for help.   