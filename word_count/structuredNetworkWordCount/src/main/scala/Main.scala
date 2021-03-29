import java.util.SplittableRandom

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Read stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" ")) // Convert DataFrame to Dataset of Strings

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}
