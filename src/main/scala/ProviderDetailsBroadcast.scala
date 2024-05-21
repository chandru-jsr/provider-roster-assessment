import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * This program assumes Provider data will be constant, so it shall be broadcast to the worker node
 * Visits and Provider repartioning by partition_id so that operation functions on same workers
 */
object ProviderDetailsBroadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProviderDetailsBroadcast")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val providersDF = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("data/providers.csv")
      .select($"provider_id", $"first_name", $"last_name", $"provider_specialty")
      .as[(String, String, String, String)]
      .repartition($"provider_id")

    // Broadcast Provider as provider's data will not change often
    val providerBroadcastDF=broadcast(providersDF)

    val visitsDF = spark.read
      .option("header", "false")
      .csv("data/visits.csv")
      .toDF("visit_id", "provider_id", "date_of_service")
      .select($"provider_id", $"date_of_service")
      .as[(String, String)]
      .repartition($"provider_id")

    // Calculate total number of visits per provider per specialty
    val visitsPerProvider = visitsDF.groupBy("provider_id").count()
    // Joining visits with broadcast provider
    val providerVisits = visitsPerProvider.join(providerBroadcastDF, Seq("provider_id"))
      .select($"provider_id", $"first_name", $"last_name", $"provider_specialty", $"count".alias("total_visits"))

    // Output partitioned by specialty
    providerVisits.write.partitionBy("provider_specialty").json("output/provider_visits_by_specialty_broadcast")

    // Calculate total number of visits per provider per month
    val visitsPerProviderPerMonth = visitsDF.withColumn("month", date_format($"date_of_service", "yyyy-MM"))
      .groupBy("provider_id", "month").count()

    // Output report
    visitsPerProviderPerMonth.write.json("output/provider_visits_by_month_broadcast")

    spark.stop()
  }
}
