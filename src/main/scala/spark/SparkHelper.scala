package spark

import com.typesafe.config.ConfigFactory
import log.LazyLogger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkHelper extends LazyLogger{
  log.warn("=======> Starting SparkHelper ....")
  val config = ConfigFactory. load
  def getAndConfigureSparkSession() = {
    val conf = new SparkConf()
      .setAppName("Structured Streaming from Parquet to Cassandra")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", config.getString("cassandra.connection.host"))
      .set("spark.sql.streaming.checkpointLocation", "checkpoint")
      .set("es.nodes",config.getString("es.nodes") ) // full config : https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
      .set("es.index.auto.create", "true") //https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
      .set("es.nodes.wan.only", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    SparkSession
      .builder()
      .getOrCreate()
  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()
  }
}
