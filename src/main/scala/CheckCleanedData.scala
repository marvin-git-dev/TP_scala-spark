import org.apache.spark.sql.SparkSession

object CheckCleanedData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("CheckCleanedData")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val cleanedDF = spark.read.parquet("C:\\Users\\mlawal\\Documents\\cleaned_bookcorpus")
    cleanedDF.show(20)
    spark.stop()
  }
}
