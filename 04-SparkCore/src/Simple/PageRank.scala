package Simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PageRank {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]) {
    val filePath = "D:/dev/GitHub/BEAD2020June/04-SparkCore/data/input.txt"
    val conf = new SparkConf().setAppName("Page Rank").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(filePath) // read text file into RDD 
    val paris = lines.map{s => 
      val parts = s.split(" ") //Splits a line into an array of 2 elements according space(s)
      (parts(0),parts(1))      //Create the parts<url0, url1> for each line in the file
    }
    
    val links = paris.distinct().groupByKey().cache() //RDD1 <string, string> -> RDD2<string, iterable> 
                                                      //Create the links table
    
    var ranks = links.mapValues(v => 1.0) //create the ranks <key,one> RDD from the links <key, Iter> RDD
    for (i <-1 to 10){
      val contribs = links.join(ranks)   // join links and ranks to create RDD1 
      .values                            // extract values from RDD1 to create RDD2
      .flatMap{case (urls, rank) =>      //RDD2 to create contribs
        val size = urls.size
        urls.map(url => (url, rank/size)) //the the ranks are distributed equally among the various URLs
    }
      ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _) //calculate the rank RDD    
    }
    ranks.foreach(println)
  }
}