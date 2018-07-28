sealed abstract class MergeAlgo( val sqlc: SQLContext ) {
  def apply( network: Network, logFile: LogFile ): ( Network, DataFrame )
  def calCodeLength( modules: DataFrame ) = {
    sqlc.udf.register( "plogp", MergeAlgo.plogp )
    if( modules.groupBy.count > 1 ) {
      val plogp_sum_q = plogp( modules.groupBy.sum('exitq).head.getDouble(0) )
      val sum_plogp_q = -2*modules.select( plogp('exitq) ).groupBy.sum
      val sum_plogp_p = -modules.select( plogp('prob) ).groupBy.sum
      val sum_plogp_pq = modules.select( plogp('prob+'exitq) ).groupBy.sum
      plogp_sum_q +sum_plogp_q +sum_plogp_p +sum_plogp_pq
    }
    else
    // if the entire graph is merged into one module,
    // there is easy calculation
      -ergodicFreqSum
  }
}

object MergeAlgo {
  /***************************************************************************
   * simple merge algorithm factory
   ***************************************************************************/
  def choose( algo: String ): MergeAlgo =
    if( algo == "InfoMap" )
      new InfoMap
    else if( algo == "InfoFlow" )
      new InfoFlow
    else
      throw new Exception( "Merge algorithm must be:"
        +"InfoMap or InfoFlow" )

  /***************************************************************************
   * math functions to calculate code length
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
        deltaLi +Partition.plogp( qi_sum +delta_q ) -Partition.plogp(qi_sum)
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
