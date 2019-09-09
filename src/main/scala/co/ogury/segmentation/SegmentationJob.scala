package co.ogury.segmentation

import java.sql.Date
import java.time.LocalDate
import java.nio.file.Paths

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

object SegmentationJob {

  def main(args: Array[String]): Unit = {

    val dataPath = args(0)
    val outputFile = args(1)
    val endDate = args(2)
    val segmentationPeriod = args(3)

    val session = createSparkSession()
    try
    {
      run(session, dataPath, outputFile, LocalDate.parse(endDate), segmentationPeriod.toInt)
    }
    catch
      {
        case _: Throwable => println("Got some other kind of exception")
      }
    finally
    {
      val idUdf = udf { Random.alphanumeric.take(7).map(_.toLower).mkString}
      session.stop()
    }
  }

  def run(session: SparkSession, dataPath: String, outputFile: String, endDate: LocalDate, segmentationPeriod: Int): Unit = {
    val customers = loadCustomers(session, dataPath)
    val transaction = loadTransactions(session, dataPath)

    val segmentation = computeSegmentation(customers, transaction, Date.valueOf(endDate.minusDays(segmentationPeriod)), Date.valueOf(endDate))

    saveSegmentation(segmentation, outputFile)

  }


  private def loadTransactions(session: SparkSession, dataPath: String): Dataset[Transaction] = {
    import session.implicits._

    val transactions = session.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Paths.get(dataPath, "transactions").toString)
      .as[Transaction]
    return transactions
  }

  private def loadCustomers(session: SparkSession, dataPath: String): Dataset[String] = {
    import session.implicits._

    val customers = session.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(Paths.get(dataPath, "customers").toString)
      .as[String]
    return customers
  }

  def computeSegmentation(customers: Dataset[String], transactions: Dataset[Transaction], startDate: Date, endDate: Date): DataFrame = {
    val customersActivity = customers.join(
      transactions,
      customers("customerId") === transactions("customerId"),
      "left"
    ).select(customers("customerId"), transactions("date")).toDF("customerId", "date").select(
      col("customerId"),
      from_unixtime(unix_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")).cast("date").alias("date")
    ).withColumn(
      "before", col("date") < startDate
    ).withColumn(
      "during", col("date") >= startDate &&  col("date") <= endDate
    )
    val seg = customersActivity.groupBy(col("customerId")).agg(
      max("before").alias("before"),
      max("during").alias("during")
    ).select(
      col("customerId").alias("customer-id"),
      when(
        col("before") && col("during"), lit("active")
      ).when(
        col("before"), lit("inactive")
      ).when(
        col("during"), lit("new")
      ).otherwise("undefined").alias("activity-segment")
    )
    return seg
  }

  private def saveSegmentation(segmentation: DataFrame, outputFile: String): Unit = {
    val tmpDir = Paths.get(outputFile).getParent.toString

    segmentation.repartition(1).write.option("header", "true").option("delimiter", ";").csv(
      tmpDir
    )

    val tmpFile = Paths.get(tmpDir).toFile.listFiles().filter(
      file => file.getName.startsWith("part") && file.getName.endsWith("csv")
    ).toList.head

    tmpFile.renameTo(Paths.get(outputFile).toFile)

  }

  private def createSparkSession(): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark of segmentation job")
      .master("local[*]")
      .getOrCreate()

    return spark
  }


}
