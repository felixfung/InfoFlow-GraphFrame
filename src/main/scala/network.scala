  /***************************************************************************
   * store graph data that are relevant to community detection
   * which involves a few scalar (Double or Long) variables
   * and a GraphFrame
   * importantly, the GraphFrame object stores reduced graph
   * where each node represents a module/community
   * this reduced graph can be combined with the original graph
   * given a partitioning mapping each nodal index to a modular index
   ***************************************************************************/

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

sealed case class Network
(
  tele: Double, // PageRank teleportation chance
  nodeNumber: Long, // number of vertices/nodes in network
  // graph: GraphFrame:
  // vertices: modular properties
  // vertices: | idx , prob , exitw , exitq |
  // (module index) (ergidc frequency) (exit prob w/o tele) (exit prob w/ tele)
  // edges: transition probability w/o tele
  // deges: | idx1 , idx2 , exitw |
  graph: GraphFrame,
  // sum_node plogp(prob), for codelength calculation
  // it can only be calculated with the full graph and not the reduced one
  // therefore it is calculated during Network.init()
  // and stored here
  probSum: Double,
  codelength: Double // information theoretic entropy
)

object Network
{
  /***************************************************************************
   * given a graph/GraphFrame (probably from GraphFile.graph)
   * and the PageRank teleportation probability
   * calculate PageRank and exit probabilities for each node
   * these are put and returned to a Network object
   * which can be used for community detection
   ***************************************************************************/
  def init( graph: GraphFrame, tele: Double ): Network = {
    // graph.vertices: ( idx: Long, name: String, module: Long )
    // graph.edges: ( idx1: Long, idx2: Long, exitw: Double )

    val nodeNumber: Long = graph.vertices.groupBy().count.head.getLong(0)

    // get PageRank ergodic frequency for each node
    val prob = graph.pageRank.resetProbability(1-tele).tol(0.01).run

    // modular information
    // since transition probability is normalized per 'idx1 node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    val modules = prob.vertices.join( graph.edges,
      col("idx") === col("idx1"), "left_outer"
    )
    .select( col("idx"), lit(1) as "size", col("pagerank") as "prob",
      when(col("idx1").isNotNull,col("pagerank")).otherwise(0) as "exitw",
      when(col("idx1").isNotNull,col("pagerank")).otherwise(col("tele")*col("pagerank")) as "exitq"
    )

    // probability of transitioning within two modules w/o teleporting
    // the merging operation is symmetric towards the two modules
    // identify the merge operation by
    // (smaller module index,bigger module index)
    val edges = prob.vertices.join(
      graph.edges.filter( "idx1 != idx2" ), // filter away self connections
      "idx === idx1"
    )
    .select(col("idx1"),col("idx2"),col("pagerank")*col("exitw"))

    // calculate current code length
    val probSum: Double = modules.select( CommunityDetection.plogp()(col("prob") ))
    .groupBy().sum("prob")
    .head.getDouble(0)
    val codelength: Double = CommunityDetection.calCodelength( modules, probSum )

    Network(
      tele, nodeNumber,
      GraphFrame(modules,edges),
      probSum,
      codelength
    )
  }

  // function to trim RDD/DF lineage
  // which should be performed within community detection algorithm iterations
  // to avoid stack overflow problem
  def trim( df: DataFrame ): Unit = {
    /*import org.apache.spark.sql.SparkSession
    val spark = SparkSession .builder() .getOrCreate()
    import spark.implicits._*/

    df.rdd.checkpoint
    df.rdd.count
    //df.rdd.toDF
  }
}
