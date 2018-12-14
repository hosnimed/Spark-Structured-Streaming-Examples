package kafka

import log.LazyLogger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types._
import spark.SparkHelper

object KafkaService extends LazyLogger{
  private val spark = SparkHelper.getSparkSession()

  val radioStructureName = "radioCount"

  val topicName = "test"
  val bootstrapServers =SparkHelper.config.getString("kafka.bootstrapServers")
  log.warn(s"KafkaService : BootstrapServers :${bootstrapServers}")
  val schemaOutput = new StructType()
    .add("title", StringType)
    .add("artist", StringType)
    .add("radio", StringType)
    .add("count", LongType)
}
