  /***************************************************************************
   * routine to perform functional testing for community detection algorithms
   * handles file reading, network construction, community detection
   * and checking numerical results
   ***************************************************************************/

import org.apache.spark.sql._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

object CommunityDetectionTest
{
  def apply(
    ss: SparkSession,
    inputFile: String,
    communityDetection: CommunityDetection,
    partitionExpected: Set[Row],
    codelengthExpected: Double
  ): ( Boolean, Network ) = {

    // perform calculations
    val graph0 = GraphReader( ss, inputFile )
    val net0 = Network.init( graph0, 0.15 )
    val logFile = new LogFile("log.txt","","","",false,false,true)
    val (net1,graph1) = communityDetection( net0, graph0, logFile )
graph0.vertices.show
graph1.vertices.show
    // check partitioning
    val partitionResult = graph1.vertices
    .select( col("id"), col("module") )
    .collect.toSet
    val partitionSuccess: Boolean = partitionResult == partitionExpected
println(net1.codelength)
    // check codelength
    val codelengthSuccess: Boolean =
      Math.abs( codelengthExpected -net1.codelength ) <= 0.1

    ( partitionSuccess && codelengthSuccess, net1 )
  }
}
