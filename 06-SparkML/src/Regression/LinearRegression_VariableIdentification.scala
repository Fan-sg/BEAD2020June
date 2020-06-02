package Regression

import org.apache.spark._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.sql.types._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object LinearRegression_VariableIdentification {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Sales_Prediction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //Loading data
    val sales_data_train =
      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema","true")
        .load("C:/dev/github/BEAN2020June/06-SparkML/data/regression/Sales_Train.csv")
    val sales_data_test =
      sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema","true")
        .load("C:/dev/github/BEAN2020June/06-SparkML/data/regression/Sales_Test.csv")
    val sales_data_union = sales_data_train.unionAll(sales_data_test)
    val sales_data = sales_data_union.withColumn("Item_Outlet_Sales",
      sales_data_union.col("Item_Outlet_Sales").cast(DoubleType))
    sales_data.show(5) 
    }

}