import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class NetworkTest extends FunSuite
{
  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("InfoFlow Pajek file tests")
    .config("spark.master","local[*]")
    .getOrCreate

  var sc: SparkContext = _
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import spark.implicits._

  /***************************************************************************
   * Test Cases
   ***************************************************************************/

  test("Initialize network") {
    val vertices = List( (1,"1",1), (2,"2",2) ).toDF("id","name","module")
    val edges = List( (1,2,1), (2,1,1) ).toDF("src","dst","exitw")
    val graph0 = GraphFrame( vertices, edges )
    val network = Network.init( graph0, 0.15 )
    assert( network.tele === 0.15 )
    assert( network.nodeNumber ===
      network.graph.vertices.groupBy().count.head.getLong(0) )
    //assert( network.probSum === ??? )
    //assert( network.codelength === ??? )
    //assert( network.graph.vertices.select('id,'prob).collect.toSet === ??? )
  }
}
