import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class PajekReaderTest extends FunSuite with BeforeAndAfter
{
  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/

  var sc: SparkContext = _
  val spark = SparkSession
    .builder()
    .appName("InfoFlow Pajek file tests")
    .config("spark.master","local[*]")
    .getOrCreate
  spark.sparkContext.setLogLevel("OFF")
  import spark.implicits._
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
    assert( graph0.vertices.collect.toSet ===
      Set( Row(1,"v1",1) )
    )
    assert( graph0.edges.collect.toSet === Set() )
  }

  test("Read trivial network") {
    val graph0 = PajekReader(sqlContext,"Nets/trivial.net")
    assert( graph0.vertices.collect.toSet ===
      Set( Row(1,"m01",1), Row(2,"m02",2) )
    )
    assert( graph0.edges.collect.toSet === Set( Row(1,2,2) ) )
  }

  test("Read trivial networks with self loop") {
    val graph0 = PajekReader(sqlContext,"Nets/trivial-with-self-loop.net")
    assert( graph0.vertices.collect.toSet ===
      Set( Row(1,"m01",1), Row(2,"m02",2) )
    )
    assert( graph0.edges.collect.toSet === Set( Row(1,2,2), Row(2,2,1) ) )
  }

  test("Read simple network") {
    val graph0 = PajekReader(sqlContext,"Nets/simple.net")
    assert( graph0.vertices.collect.toSet ===
      Set( Row(1,"1",1), Row(2,"2",2), Row(3,"3",3),
           Row(4,"4",4), Row(5,"5",5), Row(6,"6",6) )
    )
    assert( graph0.edges.collect.toSet === Set(
      Row(1,2,1), Row(1,3,1), Row(2,1,1), Row(2,3,1), Row(3,1,1),
      Row(3,2,1), Row(3,4,0.5), Row(4,3,0.5), Row(4,5,1), Row(4,6,1),
      Row(5,4,1), Row(5,6,1), Row(6,4,1), Row(6,5,1)
    ) )
  }

  test("Read file with *edgeslist format") {
    val graph0 = PajekReader(sqlContext,"Nets/edge-test.net")
    assert( graph0.vertices.collect.toSet ===
      Set( Row(1,"1",1), Row(2,"2",2), Row(3,"3",3),
           Row(4,"4",4), Row(5,"5",5), Row(6,"6",6) )
    )
    assert( graph0.edges.collect.toSet === Set(
      Row(1,2,1), Row(1,3,1), Row(1,4,1),
      Row(2,1,1), Row(2,2,1), Row(2,6,1)
    ) )
  }

  test("Test reading arcs list") {
    val graph0 = PajekReader(sqlContext,"Nets/arcslist-test.net")
    assert( graph0.vertices.collect.toSet ===
      Set( Row(1,"1",1), Row(2,"2",2), Row(3,"3",3),
           Row(4,"4",4), Row(5,"5",5), Row(6,"6",6) )
    )
    graph0.edges.show
    assert( graph0.edges.collect.toSet === Set(
      Row(1,2,1), Row(1,3,1), Row(1,4,1),
      Row(2,1,1), Row(2,2,1), Row(2,6,1),
      Row(3,2,1), Row(3,4,1)
    ) )
  }

  /***************************************************************************
   * Terminate Spark Context
   ***************************************************************************/
  after {
    if( sc != null ) sc.stop
  }
}
