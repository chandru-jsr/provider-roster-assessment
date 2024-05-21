import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProviderDetails {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProviderDetails")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val providersDF = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv("data/providers.csv")

    val visitsDF = spark.read
      .option("header", "false")
      .csv("data/visits.csv")
      .toDF("visit_id", "provider_id", "date_of_service")

    // Calculate total number of visits per provider per specialty
    val visitsPerProvider = visitsDF.groupBy("provider_id").count()

    val providerVisits = providersDF.join(visitsPerProvider, Seq("provider_id"))
      .select($"provider_id", $"first_name", $"last_name", $"provider_specialty", $"count".alias("total_visits"))

    // Output partitioned by specialty
    providerVisits.write.partitionBy("provider_specialty").json("output/provider_visits_by_specialty")

    // Calculate total number of visits per provider per month
    val visitsPerProviderPerMonth = visitsDF.withColumn("month", date_format($"date_of_service", "yyyy-MM"))
      .groupBy("provider_id", "month").count()

    // Output report
    visitsPerProviderPerMonth.write.json("output/provider_visits_by_month")

    spark.stop()
  }
}
