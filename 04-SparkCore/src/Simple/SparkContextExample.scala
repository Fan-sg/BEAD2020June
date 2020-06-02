package Simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextExample {
    def main(args: Array[String]) {
    val logFile =
        "C:/dev/github/BEAN2020June/04-SparkCore/data/names.txt"
    val conf = new SparkConf()
             .setAppName("RDD Examples").setMaster("local[*]")
    val sc = new SparkContext(conf)
val logData = sc.textFile(logFile, 2).cache()
val numAs = logData.filter(line => line.contains("a")).count()
val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs,numBs))
  }

}