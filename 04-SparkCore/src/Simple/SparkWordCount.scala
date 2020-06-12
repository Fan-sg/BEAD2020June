package Simple


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val f = sc.textFile("D:/dev/GitHub/BEAD2020June/04-SparkCore/data/words.txt") 
    val wc = f.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _) 
    val partNum = wc.partitions.size
    println(partNum)
}
}