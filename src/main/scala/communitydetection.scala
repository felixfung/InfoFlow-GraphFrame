  /***************************************************************************
   * abstract base class for community detection algorithm
   *
   * there is only one apply() function as interface
   * no state is stored
   * hence, this class/object is not necessary
   * but can be replaced with simple function and function pointer
   * the reason to use class/object is:
   *   (1) organization: to group the apply() function and helper functions
   *   (2) object factory: again arguably for better code organization
   ***************************************************************************/

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.log2

abstract class CommunityDetection {
  // real meat of community detection algorithm
  // Network object holds all relevant community detection variables
  // returns also a GraphFrame of the original graph
  // these two data structures together holds community data and graph data
  // GraphFrame: vertex names, communities/partitions
  // Network: community/partition size, PageRank/ergodic frequency,
  //   and exit probability between vertices w/o teleportation
  def apply(
    network: Network, graph: GraphFrame,
    logFile: LogFile
  ): ( Network, GraphFrame )
}

object CommunityDetection {
  /***************************************************************************
   * simple merge algorithm factory to return CommunityDetection object
   ***************************************************************************/
  def choose( algo: String ): CommunityDetection =
    if( algo == "InfoMap" )
      new InfoMap
    else if( algo == "InfoFlow" )
      new InfoFlow
    else
      throw new Exception( "Merge algorithm must be:"
        +"InfoMap or InfoFlow" )

  // calculate code length given modular properties
  // and the sum_node of plogp(prob) (can only be calculated with full graph)
  def calCodelength( modules: DataFrame, probSum: Double ): Double = {
    if( modules.groupBy().count.head.getLong(0) > 1 ) {
      modules.cache
      def plogpd( double: Double ): Double = {
        def log( double: Double ) = Math.log(double)/Math.log(2.0)
        double *log( double )
      }
      val plogp_sum_q = plogpd( modules.groupBy().sum("exitq")
        .head.getDouble(0) )
      val sum_plogp_q = -2* modules.select( plogp()(col("exitq")) as "plogp_q")
        .groupBy().sum("plogp_q")
        .head.getDouble(0)
      val sum_plogp_pq = modules.select(
        plogp()( col("prob") +col("exitq") )
      as "plogp_pq" )
        .groupBy().sum("plogp_pq")
        .head.getDouble(0)
      plogp_sum_q +sum_plogp_q -probSum +sum_plogp_pq
    }
    else {
    // if the entire graph is merged into one module,
    // there is easy calculation
      -probSum
    }
  }

  def calDeltaL()(
    nodeNumber: Column,
    n1: Column, n2: Column, p1: Column, p2: Column,
    tele: Column, qi_sum: Column, q1: Column, q2: Column,
    w12: Column,
    probSum: Column, codelength: Column
  ): Column = {
    val q12 = calQ()( tele, nodeNumber, n1+n2, p1+p2, w12 )
    when( q12 > lit(0),
      plogp()( qi_sum +q12-q1-q2 ) -plogp()( qi_sum )
      -lit(2.0)*( plogp()(q12) -plogp()(q1) -plogp()(q2) )
      +plogp()(p1+p2+q12) -plogp()(p1+q1) -plogp()(p2+q2)
    )
    // if there is only one module, then the standard formula doesn't work
    // and needs to calculate dL via network.probSum - current codelength
    .otherwise(
      -probSum -codelength
    )
  }
 
  def calQ()(
    tele: Column,
    nodeNumber: Column, size: Column, prob: Column,
    exitw: Column
  ): Column = (
    tele *(nodeNumber-size) /(nodeNumber-lit(1)) *prob // teleportation
    +(lit(1)-tele) *exitw                              // random walk
  )

  def plogp()( double: Column ): Column = {
    double *log2( double )
  }
}
