  /***************************************************************************
   * store graph data that are relevant to community detection
   * which involves a few scalar (Double or Long) variables
   * and a GraphFrame
   * importantly, the GraphFrame object stores reduced graph
   * where each node represents a module/community
   * this reduced graph can be combined with the original graph
   * given a partitioning mapping each nodal index to a modular index
   ***************************************************************************/

sealed case class Network
(
  tele: Double, // PageRank teleportation chance
  nodeNumber: Long, // number of vertices/nodes in network
  graph: GraphFrame,
  // vertices: modular properties
  // vertices: | idx , prob , exitw , exitq |
  // (module index) (ergidc frequency) (exit prob w/o tele) (exit prob w/ tele)
  // edges: transition probability w/o tele
  // deges: | idx1 , idx2 , exitw |
  codelength: Double // information theoretic entropy
)
{
  // function to trim RDD/DF lineage
  // which should be performed within community detection algorithm iterations
  // to avoid stack overflow problem
  def localCheckpoint = {
    def trim( df: DataFrame ): Unit = {
      df.rdd.checkpoint
      df.rdd.count
      val newDf = df.rdd.toDF( df.schema )
    }
    trim( graph.vertices )
    trim( graph.edges )
  }
}

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

    // filter away self connections
    graph.edges = graph.edges.filter( 'idx1 != 'idx2 )

    val nodeNumber: Long = graph.vertices.groupBy.count('idx)

    // get PageRank ergodic frequency for each node
    val prob = graph.pageRank.resetProbability(1-tele).tol(0.01).run

    // modular information
    // since transition probability is normalized per 'idx1 node,
    // w and q are mathematically identical to p
    // as long as there is at least one connection
    val modules = prob.join( graph.edges,
      prob('idx) === graph.edges('idx1), "left_outer"
    )
    .select( 'idx, 1 as "size", col("pagerank") as "prob",
      when('idx1.isNotNull,pagerank).otherwise(0) as "exitw",
      when('idx1.isNotNull,pagerank).otherwise(tele*'pagerank) as "exitq"
    )

    // probability of transitioning within two modules w/o teleporting
    // the merging operation is symmetric towards the two modules
    // identify the merge operation by
    // (smaller module index,bigger module index)
    val edges = prob.vertices.join(
      graph.edges, prob('idx) === graph.edges('idx1)
    )
    .drop('idx) // remove duplicate index
    .select('idx1,'idx2,'pagerank*'exitw)

    // calculate current code length
    val codeLength: Double = calCodeLength(modules)
  }

  Network(
    tele, nodeNumber,
    GraphFrame(modules,edges),
    codeLength,
    graph.vertices("name")
  )
}
