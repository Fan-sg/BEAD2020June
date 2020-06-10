package Simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkDemo {
   def main(args: Array[String]) {
    val stocksPath = "D:/dev/GitHub/BEAD2020June/04-SparkCore/data/linecount.txt"
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile(stocksPath, 2)
    val totalLines = data.count()
    println("Total number of Lines: %s".format(totalLines))
  }
}