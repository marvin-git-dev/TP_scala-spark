import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BookCorpusPreprocessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BookCorpusPreprocessing")
      .config("spark.master", "local[*]")
      .getOrCreate()

   
    val filePath1 = "C:\\Users\\mlawal\\Documents\\bookcorpus\\books_large_p1.txt"
    val filePath2 = "C:\\Users\\mlawal\\Documents\\bookcorpus\\books_large_p2.txt"
    
   
    val bookCorpusDF1 = spark.read.text(filePath1)
    val bookCorpusDF2 = spark.read.text(filePath2)
    
    
    val bookCorpusDF = bookCorpusDF1.union(bookCorpusDF2)

    
    val cleanedDF = bookCorpusDF.filter(row => row.getAs[String]("value").trim.nonEmpty)

    
    cleanedDF.show(10)

    
    cleanedDF.write.mode("overwrite").parquet("C:\\Users\\mlawal\\Documents\\cleaned_bookcorpus")
    
    spark.stop()
  }
}
