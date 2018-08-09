import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class NetworkTest extends FunSuite with BeforeAndAfter
{

  /***************************************************************************
   * this test suite is mostly testing for numerical calculation correctness
   * hence, here define floating point equality within Row(...)
   ***************************************************************************/
  def equalWithTolerance(
    df1: List[Row], df2: List[Row], tolerance: Double,
    firstColumn: Int ): Boolean = {
    df1 match {
      // if df1 is done, so should df2
      case Nil => {
        df2 match {
          case Nil => true
          case _ => false
        }
      }
      // if df1 is not empty, df2 should not be empty
      // and each row element should be a double (other than the first column)
      // and each corresponding column between df1 and df2 should be equal
      // within tolerance
      case row1 :: list1 => {
        df2 match {
          case Nil => false
          case row2 :: list2 => {
            val length = row1.length
            if( row2.length != length ) false
            else {
              // use iterative loop here to check for equality
              var equal = true
              for( i <- firstColumn to length-1 if equal )
                equal = (
                  Math.abs( row1.getDouble(i) -row2.getDouble(i) ) <= tolerance
                )
              equal && equalWithTolerance(list1,list2,tolerance,firstColumn)
            }
          }
        }
      }
    }
  }
  def modulesEq( mod1: List[Row], mod2: List[Row],
    tolerance: Double ): Boolean = {
    equalWithTolerance( mod1, mod2, tolerance, 2 )
  }
  def edgesEq( edge1: List[Row], edge2: List[Row],
    tolerance: Double ): Boolean = {
    equalWithTolerance( edge1, edge2, tolerance, 2 )
  }
  /***************************************************************************
   * Initialize Spark Context
   ***************************************************************************/

  var sc: SparkContext = _
  val spark = SparkSession
    .builder()
    .appName("InfoFlow network intiation tests")
    .config("spark.master","local[*]")
    .getOrCreate
  spark.sparkContext.setLogLevel("OFF")
  import spark.implicits._
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  /***************************************************************************
   * Test Cases
   ***************************************************************************/

  test("Check calculations for trivial network") {
    val vertices = List( (1,"1",1), (2,"2",2) ).toDF("id","name","module")
    val edges = List( (1,2,1), (2,1,1) ).toDF("src","dst","exitw")
    val graph0 = GraphFrame( vertices, edges )
    val network = Network.init( graph0, 0.15 )
    assert( network.tele === 0.15 )
    assert( network.nodeNumber ===
      network.graph.vertices.groupBy().count.head.getLong(0) )
    assert(edgesEq(
      network.graph.edges.orderBy("src","dst").collect.toList,
      List( Row(1,2,0.5), Row(2,1,0.5) ),
      0.1
    ))
    assert(modulesEq(
      network.graph.vertices.orderBy("id").collect.toList,
      List( Row(1,1,0.5,0.5,0.5), Row(2,1,0.5,0.5,0.5) ),
      0.1
    ))
    //assert( network.probSum === ??? )
    //assert( network.codelength === ??? )
    //assert( network.graph.vertices.select('id,'prob).collect.toSet === ??? )
  }

  ignore("Check calculations for trivial network with self loop (PageRank?)") {
  }

  /***************************************************************************
   * Terminate Spark Context
   ***************************************************************************/
  after {
    if( sc != null ) sc.stop
  }
}
