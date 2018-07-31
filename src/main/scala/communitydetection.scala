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

sealed abstract class CommunityDetection {
  // real meet of community detection algorithm
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

  /***************************************************************************
   * math functions to calculate code length
   * these are all pure functions
   * so that given the same function arguments
   * will always return the same result
   ***************************************************************************/

  // calculate code length given modular properties
  // and the sum_node of plogp(prob) (can only be calculated with full graph)
  def calCodelength( modules: DataFrame, probSum: Double ) = {
    if( modules.groupBy.count > 1 ) {
      val plogp_sum_q = plogp( modules.groupBy.sum('exitq)
        .head.getDouble(0) )
      val sum_plogp_q = -2*modules.select( plogp('exitq) ).groupBy.sum('exitq)
        .head.getDouble(0)
      val sum_plogp_pq = modules.select( plogp('prob+'exitq) as "pq" )
        .groupBy.sum('pq)
        .head.getDouble(0)
      plogp_sum_q +sum_plogp_q +probSum +sum_plogp_pq
    }
    else
    // if the entire graph is merged into one module,
    // there is easy calculation
      -probSum
  }

  // calculates the change in code length
  // when two modules are merged
  def calDeltaL(
    nodeNumber: Long,
    n1: Column, n2: Column, p1: Column, p2: Column,
    tele: Column, qi_sum: Column, q1: Column, q2: Column,
    w12: Column
  ) = {
    val q12 = calQ( nodeNumber, n1+n2, p1+p2, tele, w12 )
    calDeltaL_v(qi_sum,q1,q2,q12) +calDeltaL_nv(p1,p2,q1,q2,q12)
  }

  // calculates the probabilty of exiting a module (including teleportation)
  def calQ(
    tele: Double
    nodeNumber: Long, size: Long, prob: Double,
    exitw: Double
  ): Double = (
    tele *(nodeNumber-size) /(nodeNumber-1) *prob // teleportation
    +(1-tele) *exitw                              // random walk
  )

  /***************************************************************************
   * when calculating the code length
   * one component depends on qi_sum, one does not
   * so that one is "volatile" and needs recalculating per iteration
   * while the other is "non-volatile"
   * to save computation, divide these into different functions
   * to get deltaL, simply add the two together
   ***************************************************************************/

  // volatile term of change in code length
  // argument qi_sum IS a dependency
  def calDeltaL_v( qi_sum: Column, q1: Column, q2: Column, q12: Column ) = (
    +plogp( qi_sum +q12-q1-q2 )
    -plogp( qi_sum )
  )

  // NON-volatile term of change in code length
  // argument qi_sum is NOT a dependency
  def calDeltaL_nv(
    p1: Column, p2: Column,
    q1: Column, q2: Column, q12: Column
  ) = (
    -2*plogp(q12) +2*plogp(q1) +2*plogp(q2)
    +plogp(p1+p2+q12) -plogp(p1+q1) -plogp(p2+q2)
  )

  /***************************************************************************
   * simple functions for codelength calculation
   * duplicate each function, each with argument type Double and Column
   * so that they can be applied to normal Double
   * or within DataFrame.select()
   ***************************************************************************/
  def log( double: Double ) = Math.log(double)/Math.log(2.0)
  def log( double: Column ) = Math.log(double)/Math.log(2.0)
  def plogp( double: Double ) = double*log(double)
  def plogp( double: Column ) = double*log(double)
}
