import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.scalactic.TolerantNumerics

class PajekFileTest extends FunSuite
{
  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("InfoFlow Pajek file tests")
    .config("spark.master","local")
    .getOrCreate

  var sc: SparkContext = _
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  /***************************************************************************
   * Test Cases
   ***************************************************************************/

  test("Throw error when reading wrong file") {
    val thrown = intercept[Exception] {
      val dummy = PajekReader(sqlContext,"Nets/dummy")
    }
    assert( thrown.getMessage === "Cannot open file Nets/dummy" )
  }

  test("Read trivial network with comment") {
    val graph0 = PajekReader(sqlContext,"Nets/zero.net")
    assert( graph0.vertices.collect.toList ===
      List( Row(1,"v1",1) )
    )
  }
}
