# hadoop-ceph
Implementation of Hadoop file system
## Ceph:RGW

Delta Lake has built-in support for the various Ceph:RGW object storage systems with full transactional guarantees for concurrent reads and writes from multiple clusters. Delta Lake relies on Hadoop FileSystem APIs to access Ceph:RGW storage services.

In this section:

- Requirements
- Quickstart
- Configuration

### Requirements

- Ceph:RGW swift user credentials: user，secret_key , endpoint.
- Apache Spark associated with the corresponding Delta Lake version.
- Hadoop’s [Ceph connector (hadoop-cephrgw)](https://search.maven.org/artifact/io.github.nanhu-lab/hadoop-cephrgw) for the version of Hadoop that Apache Spark is compiled for.

### Quickstart

This section explains how to quickly start reading and writing Delta tables on Ceph:RGW. For a detailed explanation of the configuration, see [Configuration](https://docs.delta.io/latest/delta-storage.html#-configuration).

1. Use the following command to launch a Spark shell with Delta Lake and Ceph:RGW support (assuming you use Spark pre-built for Hadoop 3.2):

   ```bash
   bin/spark-shell \
    --packages io.delta:delta-core_2.12:1.1.0,io.github.nanhu-lab:hadoop-cephrgw:1.0.3 \
    --conf spark.hadoop.fs.ceph.username=<your-cephrgw-username> \
    --conf spark.hadoop.fs.ceph.password=<your-cephrgw-password> \
    --conf spark.hadoop.fs.ceph.uri=<your-cephrgw-uri> \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore \
    --conf spark.hadoop.fs.ceph.impl=org.apache.hadoop.fs.ceph.rgw.CephStoreSystem
   ```

2. Try out some basic Delta table operations on Ceph:RGW (in Scala):

```bash
// Create a SparkSession
val spark = SparkSession.builder().appName("Quickstart").master("local[*]")
      .config("spark.hadoop.fs.ceph.username","<your-cephrgw-username>")
      .config("spark.hadoop.fs.ceph.password", "<your-cephrgw-password>")
      .config("spark.hadoop.fs.ceph.uri", "<your-cephrgw-uri>")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.hadoop.fs.ceph.impl", "org.apache.hadoop.fs.ceph.rgw.CephStoreSystem")
      
// Create a Delta table on Ceph:RGW
spark.range(5).write.format("delta").save("ceph://<your-cephrgw-container>/<path-to-delta-table>")

// Read a Delta table on Ceph:RGW
spark.read.format("delta").load("ceph://<your-cephrgw-container>/<path-to-delta-table>").show()
```

### Configuration

Here are the steps to configure Delta Lake for Ceph:RGW.

1. Include hadoop-cephrgw JAR in the classpath.

   Delta Lake needs the `org.apache.hadoop.fs.ceph.rgw.CephStoreSystem` class from the `hadoop-cephrgw` package, which implements Hadoop’s `FileSystem` API for Ceph:RGW. Make sure the version of this package matches the Hadoop version with which Spark was built.

2. Set up Ceph:RGW credentials.

   here is one way is to set up the Hadoop configurations (in Scala):

   ```bash
   sc.hadoopConfiguration.set("spark.hadoop.fs.ceph.username", "<your-cephrgw-username>")
   sc.hadoopConfiguration.set("spark.hadoop.fs.ceph.password", "<your-cephrgw-password>")
   sc.hadoopConfiguration.set("spark.hadoop.fs.ceph.uri", "<your-cephrgw-uri>")
   sc.hadoopConfiguration.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
   sc.hadoopConfiguration.set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
   sc.hadoopConfiguration.set(" spark.hadoop.fs.ceph.impl", "org.apache.hadoop.fs.ceph.rgw.CephStoreSystem")
   ```

