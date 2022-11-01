package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

import scala.concurrent.duration.Duration

object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Final Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("failOnDataLoss", false)// para que no falle cuando se destruye y crea un nuevo topic desde el docker
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    /*val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", IntegerType, nullable = false),
      StructField("app", StringType, nullable = false)
    )
    )*/
    val devicesMessageSchema: StructType = ScalaReflection.schemaFor[DevicesMessage].dataType.asInstanceOf[StructType]

    dataFrame
      .select(from_json(col("value").cast(StringType), devicesMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
  }

  override def readDevicesMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDevicesWithMetadata(devicesDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    devicesDF.as("device")
      .join(
        metadataDF.as("metadata"),
        $"device.id" === $"metadata.id"
      ).drop($"metadata.id")
  }


  override def computeDevicesSumByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "1 minute")//1 minute
      .groupBy($"antenna_id", window($"timestamp", "5 minutes"))//5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"antenna_id", $"sum_bytes", $"window.start".as("data"))
      //antenna_id TEXT, sum_bytes BIGINT, data TIMESTAMP
  }

  override def computeDevicesSumBytesByUsuario(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"id", $"bytes")
      .withWatermark("timestamp", "1 minute") //1 minute
      .groupBy($"id", window($"timestamp", "5 minutes")) //5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"id", $"sum_bytes", $"window.start".as("date"))
    //id TEXT, sum_bytes BIGINT, date TIMESTAMP
  }

  override def computeDevicesSumBytesByAplicacion(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "1 minute") //1 minute
      .groupBy($"app", window($"timestamp", "5 minutes")) //5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"app", $"sum_bytes", $"window.start".as("date"))
    //app TEXT, sum_bytes BIGINT, date TIMESTAMP
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"timestamp", $"id", $"antenna_id", $"bytes",$"app",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour"),
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
  }

  /**
   * Main for Streaming job
   *
   * @param args arguments for execution:
   *             kafkaServer topic jdbcUri jdbcMetadataTable aggJdbcTable jdbcUser jdbcPassword storagePath
   * Example:
   *   XXX.XXX.XXX.XXX:9092 antenna_telemetry jdbc:postgresql://XXX.XXX.XXX.XXXX:5432/postgres metadata antenna_agg postgres keepcoding /tmp/batch-storage
   */
  // Puedes descomentar este main y llamar con argumentos
  //def main(args: Array[String]): Unit = run(args)


  def main(args: Array[String]): Unit = {
    //run(args)
    val kafkaDF = readFromKafka("34.88.224.164:9092", "devices")
    val parsedDF = parserJsonData(kafkaDF)
    val storageFuture = writeToStorage(parsedDF, "/tmp/antenna_parquet/")
    val metadaDF = readDevicesMetadata(
      "jdbc:postgresql://34.88.125.105:5432/postgres",
      "PROYECTO_user_metadata",
      "postgres",
      "postgres"
    )
    val enrichDF = enrichDevicesWithMetadata(parsedDF, metadaDF)

    val sumByAntenna = computeDevicesSumByAntenna(enrichDF)
    val sumByUsuario = computeDevicesSumBytesByUsuario(enrichDF)
    val sumByAplicacion = computeDevicesSumBytesByAplicacion(enrichDF)

   /* sumByAplicacion
      .writeStream
      .format("console")
      .start()
      .awaitTermination()*/

    val jdbcFutureAggByAntena = writeToJdbc(sumByAntenna, "jdbc:postgresql://34.88.125.105:5432/postgres", "PROYECTO_streamingBytesPorAntena", "postgres", "postgres")
    val jdbcFutureAggByUsuario = writeToJdbc(sumByUsuario, "jdbc:postgresql://34.88.125.105:5432/postgres", "PROYECTO_streamingBytesPorUsuario", "postgres", "postgres")
    val jdbcFutureAggByApp = writeToJdbc(sumByAplicacion, "jdbc:postgresql://34.88.125.105:5432/postgres", "PROYECTO_streamingBytesPorAplicacion", "postgres", "postgres")

    Await.result(
      Future.sequence(Seq(storageFuture, jdbcFutureAggByAntena,jdbcFutureAggByUsuario,jdbcFutureAggByApp)), Duration.Inf
    )

    spark.close()
  }
}
