package Simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
   def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val f = sc.textFile("D:/dev/GitHub/BEAD2020June/04-SparkCore/data/words.txt") 
    val wc = f.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _) 
    val partNum = wc.partitions.size
    println(partNum)
    wc.saveAsTextFile("wc_out")
   }
}