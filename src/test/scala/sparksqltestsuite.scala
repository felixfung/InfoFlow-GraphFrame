  /***************************************************************************
   * class that provides unit testing framework for SQL Spark
   * inherit to have Spark initialization and termination handled automatically
   * access Spark SQL and DataFrame/DataSet via sqlContext
   ***************************************************************************/

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class SparkSQLTestSuite extends FunSuite with BeforeAndAfter
{
  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  var sc: SparkContext = _
  val spark = SparkSession
    .builder()
    .appName("InfoFlow test")
    .config("spark.master","local[*]")
    .getOrCreate
  spark.sparkContext.setLogLevel("OFF")
  spark.sparkContext.setCheckpointDir("/tmp")
  import spark.implicits._
  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

  /***************************************************************************
   * Terminate Spark Context
   ***************************************************************************/
  after {
    if( sc != null ) sc.stop
  }
}
