// store entire network data
sealed case class Network
(
  tele: Double, // PageRank teleportation chance
  nodeNumber: Long, // number of vertices/nodes in network
  graph: GraphFrame, // nodes: modules, edges: transition probability w/o tele
  codelength: Double // information theoretic entropy
)

object Network
{
  /***************************************************************************
   * Normalized sparse matrix for PageRank calculation
   ***************************************************************************/
  def init( graph: GraphFrame, tele: Double ): Network = {
    // graph.vertices: ( idx: Long, name: String )
    // graph.edges: ( from: Long, to: Long, weight: Double )

    // filter away self connections
    graph.edges = graph.edges.filter( "from != to" )

    val nodeNumber: Long = graph.vertices.count

    // get PageRank ergodic frequency for each node
    val prob = graph.pageRank.resetProbability(1-tele).tol(0.01).run

    // modular information
    // since transition probability is normalized per "from" node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    val modules = prob.join( graph.edges,
      prob("idx") === graph.edges("from"), "left_outer"
    )
    .select( "idx", 1 as "size", col("pagerank") as "prob",
      when(col("from").isNotNull,pagerank).otherwise(0) as "exitw",
      when(col("from").isNotNull,pagerank).otherwise(tele*pagerank) as "exitq"
    )

    // probability of transitioning within two modules w/o teleporting
    // the merging operation is symmetric towards the two modules
    // identify the merge operation by
    // (smaller module index,bigger module index)
    val iWj = prob.vertices.join(
      graph.edges, prob("idx") === graph.edges("from")
    )
    .drop("idx") // remove duplicate index
    .select(
      when( "from"<"to", "from" ).otherwise( "to" ),
     .when( "from"<"to", "to" ).otherwise( "from" ),
      "pagerank" *"weight"
    )
    // aggregate edge weights since this is symmetric exit probability
    .groupBy( "idx1", "idx2" ).groupBy.sum("weight")

    // calculate current code length
    val codeLength: Double = {
      val qi_sum = modules.groupBy.sum("exitq")
      val ergodicFreqSum = modules.groupBy.sum("prob")
      MergeAlgo.calCodeLength( qi_sum, ergodicFreqSum, modules )
    }
  }

  Network( tele, nodeNumber, GraphFrame(modules,iWj), codeLength )
}
