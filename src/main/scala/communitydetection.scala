  /***************************************************************************
   * abstract base class for community detection algorithm
   *
   * other than a few helper functions
   * there is only one apply() function as interface
   * no state is stored
   * hence, this class/object is not necessary
   * but can be replaced with simple function and function pointer
   * the reason to use class/object is:
   *   (1) organization: to group the apply() function and helper functions
   *   (2) object factory: again arguably for better code organization
   ***************************************************************************/
sealed abstract class CommunityDetection {
  // real meet of community detection algorithm
  // Network object holds all relevant community detection variables
  // returns also a GraphFrame of the original graph
  // these two data structures together holds community data and graph data
  // GraphFrame: vertex names, communities/partitions
  // Network: community/partition size, PageRank/ergodic frequency,
  //   and exit probability between vertices w/o teleportation
  def apply( network: Network, logFile: LogFile ): ( Network, GraphFrame )

  // calculate code length given modular properties
  def calCodeLength( modules: DataFrame ) = {
    def plogp( x: Column ) = CommunityDetection.plogp(x)
    sqlc.udf.register( "plogp", CommunityDetection.plogp )
    if( modules.groupBy.count > 1 ) {
      val plogp_sum_q = CommunityDetection.plogp( modules.groupBy.sum('exitq)
        .head.getDouble(0) )
      val sum_plogp_q = -2*modules.select( plogp('exitq) ).groupBy.sum
        .head.getDouble(0)
      val sum_plogp_p = -modules.select( plogp('prob) ).groupBy.sum
        .head.getDouble(0)
      val sum_plogp_pq = modules.select( plogp('prob+'exitq) ).groupBy.sum
        .head.getDouble(0)
      plogp_sum_q +sum_plogp_q +sum_plogp_p +sum_plogp_pq
    }
    else
    // if the entire graph is merged into one module,
    // there is easy calculation
      -ergodicFreqSum
  }
}

  /***************************************************************************
   * static functions for code length calculation
   * and simple factory to return merge algorithm object
   ***************************************************************************/
object CommunityDetection {
  /***************************************************************************
   * simple merge algorithm factory
   ***************************************************************************/
  def choose( algo: String ): CommunityDetection =
    if( algo == "InfoMap" )
      new InfoMap
    else if( algo == "InfoFlow" )
      new InfoFlow
    else
      throw new Exception( "Merge algorithm must be:"
        +"InfoMap or InfoFlow" )

  /***************************************************************************
   * math functions to calculate code length
   * these are all static, pure functions
   * so that given the same function arguments
   * will always return the same result
   ***************************************************************************/

  def calQ( nodeNumber: Long, n: Long, p: Double, tele: Double, w: Double )
    = tele*(nodeNumber-n)/(nodeNumber-1)*p +(1-tele)*w

  def calDeltaL(
    nodeNumber: Long,
    n1: Long, n2: Long, p1: Double, p2: Double,
    tele: Double, w12: Double,
    qi_sum: Double, q1: Double, q2: Double
  ) = {
    val q12 = calQ( nodeNumber, n1+n2, p1+p2, tele, w12 )
    val delta_q = q12-q1-q2
    val deltaLi = (
      -2*plogp(q12) +2*plogp(q1) +2*plogp(q2)
      +plogp(p1+p2+q12) -plogp(p1+q1) -plogp(p2+q2)
    )
    val deltaL =
      if( qi_sum>0 && qi_sum+delta_q>0 )
        deltaLi
        +CommunityDetection.plogp( qi_sum +delta_q )
        -CommunityDetection.plogp( qi_sum )
      else
        0
    ( deltaLi, deltaL )
  }

  /***************************************************************************
   * math function of plogp(x) for calculation of code length
   ***************************************************************************/
  def log( double: Double ) = Math.log(double)/Math.log(2.0)
  def plogp( double: Double ) = double*log(double)
}
