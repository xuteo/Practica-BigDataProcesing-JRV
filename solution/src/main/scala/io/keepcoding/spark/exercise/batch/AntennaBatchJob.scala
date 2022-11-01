package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.xml.MetaData

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Final Exercise SQL Batch")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
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
      .groupBy($"antenna_id", window($"timestamp", "1 hour")) //5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"antenna_id", $"sum_bytes", $"window.start".as("data"))
    //antenna_id TEXT, sum_bytes BIGINT, data TIMESTAMP
  }

  override def computeDevicesSumBytesByUsuario(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .groupBy($"id", window($"timestamp", "1 hour")) //5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"id", $"sum_bytes", $"window.start".as("date"))
    //id TEXT, sum_bytes BIGINT, date TIMESTAMP
  }

  override def computeDevicesSumBytesByAplicacion(dataFrame: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"app", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour")) //5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"app", $"sum_bytes", $"window.start".as("date"))
    //app TEXT, sum_bytes BIGINT, date TIMESTAMP
  }

  override def computeUsersOverLimit(dataFrame: DataFrame, metaData: DataFrame): DataFrame = {
    dataFrame

      .select($"timestamp", $"id", $"bytes", $"quota")
      .groupBy($"id", window($"timestamp", "1 hour")) //5 minutes
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"id", $"sum_bytes", $"window.start".as("date")).as("agg")
      .join(metaData.as("metadata"), $"agg.id"===$"metaData.id")
      .select($"agg.id", $"agg.sum_bytes",$"metadata.quota", $"agg.date" )
      .where("metadata.quota < agg.sum_bytes")
    //id TEXT, sum_bytes BIGINT, quota BIGINT, date TIMESTAMP
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
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

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  /**
   * Main for Batch job
   *
   * @param args arguments for execution:
   *             filterDate storagePath jdbcUri jdbcMetadataTable aggJdbcTable aggJdbcErrorTable aggJdbcPercentTable jdbcUser jdbcPassword
   * Example:
   *   2007-01-23T10:15:30Z /tmp/batch-storage jdbc:postgresql://XXX.XXX.XXX.XXXX:5432/postgres metadata antenna_1h_agg antenna_errors_agg antenna_percent_agg postgres keepcoding
   */
  // puedes descomentar este main para pasar parametros a la ejecucion
  //def main(args: Array[String]): Unit = run(args)

  def main(args: Array[String]): Unit = {
    val jdbcUri = "jdbc:postgresql://34.88.125.105:5432/postgres"
    val jdbcUser = "postgres"
    val jdbcPassword = "postgres"

    val offsetDateTime = OffsetDateTime.parse("2022-10-30T20:00:00Z")
    val parquetDF = readFromStorage("/tmp/antenna_parquet/", offsetDateTime)
    //parquetDF.show(false)
    val metadataDF = readDevicesMetadata(jdbcUri, "PROYECTO_user_metadata", jdbcUser, jdbcPassword)
    val devicesMetadataDF = enrichDevicesWithMetadata(parquetDF, metadataDF).cache()

    val aggByAntenaDF = computeDevicesSumByAntenna(devicesMetadataDF)
    val aggByAppDF = computeDevicesSumBytesByAplicacion(devicesMetadataDF)
    val aggByUserDF = computeDevicesSumBytesByUsuario(devicesMetadataDF)
    val aggUserLimit = computeUsersOverLimit(devicesMetadataDF, metadataDF)
    devicesMetadataDF.show(false)
    aggUserLimit.show(false)

    writeToJdbc(aggByAntenaDF, jdbcUri, "PROYECTO_batchBytesPorAntena", jdbcUser, jdbcPassword)
    writeToJdbc(aggByUserDF, jdbcUri, "PROYECTO_batchBytesPorUsuario", jdbcUser, jdbcPassword)
    writeToJdbc(aggByAppDF, jdbcUri, "PROYECTO_batchBytesPorAplicacion", jdbcUser, jdbcPassword)
    writeToJdbc(aggUserLimit, jdbcUri, "PROYECTO_batchUsersOverLimit", jdbcUser, jdbcPassword)
    writeToStorage(parquetDF, "/tmp/antenna_parquet/")

    spark.close()
  }

}
