  /***************************************************************************
   * routine to perform functional testing for community detection algorithms
   * handles file reading, network construction, community detection
   * and checking numerical results
   ***************************************************************************/

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

object CommunityDetectionTest
{
  def apply(
    sqlContext: SQLContext,
    inputFile: String,
    communityDetection: CommunityDetection,
    partitionExpected: Set[Row],
    codelengthExpected: Double
  ): ( Boolean, Network ) = {

    // perform calculations
    val graph0 = GraphReader( sqlContext, inputFile )
    val net0 = Network.init( graph0, 0.15 )
    val logFile = new LogFile("log.txt","","","",false,false,true)
    val (net1,graph1) = communityDetection( net0, graph0, logFile )

    // check partitioning
    val partitionResult = graph1.vertices
    .select( col("id"), col("module") )
    .collect.toSet
    val partitionSuccess: Boolean = partitionResult == partitionExpected

    // check codelength
    val codelengthSuccess: Boolean =
      Math.abs( codelengthExpected -net1.codelength ) <= 0.1

    ( partitionSuccess && codelengthSuccess, net1 )
  }
}
