# Install Scala && Java && Maven && Spark
TiSpark is connector for Spark that enables you to use TiDB as the data source of Apache Spark, similar to other data sources. 
Before developing, you need to install the following software.
## Install Scala 2.12
We use Scala to develop TiSpark, so we need to install Scala.

We are now using Scala 2.12 . You can find out how to install Scala at [here](https://www.scala-lang.org/download/2.12.16.html).

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
## Install Spark (only for running jar)
   If you just want to run tests while developing, then you don't need to install Spark.
   However, if you wish to run TiSpark's Jar in the Spark Shell, then you need to install Spark.
   
   We support Spark 3.0, 3.1 and 3.2 in TiSpark 3.0.x.
   You can get Spark at [Downloads | Apache Spark](https://spark.apache.org/downloads.html).
   
   We will not support the installation of spark.
   If you have any questions, please turn to [Spark](https://spark.apache.org/) for help.   