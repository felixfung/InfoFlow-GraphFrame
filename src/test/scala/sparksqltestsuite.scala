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
  val ss = SparkSession
    .builder
    .appName("InfoFlow test")
    .config("spark.master","local[*]")
    .getOrCreate
  ss.sparkContext.setLogLevel("OFF")
  ss.sparkContext.setCheckpointDir("/tmp")
  import ss.implicits._
}
