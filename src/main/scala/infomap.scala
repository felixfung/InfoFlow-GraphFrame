  /***************************************************************************
   * originaL InfoMap algorithm
   * the function to partition of nodes into modules based on
   * greedily merging the pair of modules that gives
   * the greatest code length reduction
   * until code length is minimized
   ***************************************************************************/

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

sealed class InfoMap extends CommunityDetection
{
  def apply( network: Network, graph: GraphFrame, logFile: LogFile ):
  ( Network, GraphFrame ) = {

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder()
      //.appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    // sum of q's
    // used for deltaL calculations
    // will only be calculated once via full calculation
    // subsequent calculations will use dynamic programming
    network.graph.vertices.cache
    val qi_sum = network.graph.vertices.groupBy().sum("exitq").head.getDouble(0)

    // the table that lists all possible merges,
    // the modular properties of the two vertices
    // and change in code length based on the hypothetical modular merge
    // | src , dst , n1 , n2 , p1 , p2 , w1 , w2 , w1221 , q1 , q2 , dL |
    // where  n = size
    //        p = prob
    //        w = exitw
    //    w1221 = prob. going idx1->idx2 +prob. going idx2->idx1
    //        q = exitq
    //       dL = non-volatile change in codelength
    val edgeList = generateEdgeList( network, qi_sum )

  /***************************************************************************
   * greedily merge modules until code length is minimized
   ***************************************************************************/
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Long,
      network: Network, graph: GraphFrame,
      qi_sum: Double,
      edgeList: DataFrame
    ): ( Network, GraphFrame ) = {

      //  create local checkpoint to truncate RDD lineage (every ten loops)
      if( loop%10 == 0 ) trim( network, graph )

      // log current state
      logFile.write( "State " +loop.toString+": "
        +"code length " +network.codelength.toString +"\n", false )
      logFile.save( network.graph, graph, true, "net"+loop.toString )

      // if there are no modules to merge, teminate
      val count = edgeList.groupBy().count
      if( count.head.getLong(0) == 0 )
        terminate( loop, network, graph )
      else {
        val (m1,m2,n1,n2,p1,p2,w1,w2,w1221,q1,q2,deltaL) = findMerge(edgeList)
        val (m12,n12,p12,w12,q12) = calNewcal(m1,m2,n1,n2,p1,p2,w1,w2,w1221)

        if( deltaL > 0 )
          terminate( loop, network, graph )
        else { // merge two modules
          // log merging details
          logFile.write(
            "Merge " +loop.toString
            +": merging modules " +m1.toString +" and " +m2.toString
            +" with code length reduction " +deltaL.toString +"\n", false )

          // register partition to lower merge index
          val newGraph = GraphFrame(
            graph.vertices.select(
              col("id"), col("name"),
              when( col("id")===m1 || col("id")===m2, m12 )
              .otherwise( col("module") ) as "module"
            ),
            graph.edges
          )
          val newEdgeList = updateEdgeList(m1,m2,m12,n12,p12,w12,q12,edgeList)
          val newCodelength = network.codelength +deltaL
          val newNetwork = {
            val mMod = m12
            val mDel = if( m1==mMod ) m2 else m1
            calNewNetwork(
              mMod, mDel, n12, p12, w12, q12,
              network, newEdgeList, newCodelength
            )
          }
          val new_qi_sum = qi_sum +q12 -q1 -q2
          recursiveMerge( loop+1, newNetwork, newGraph, new_qi_sum, newEdgeList )
        }
      }
    }

  /***************************************************************************
   * routine finished, below are function definitions and tail recursive call
   ***************************************************************************/
      def trim( network: Network, graph: GraphFrame ): Unit = {
        Network.trim( network.graph.vertices )
        Network.trim( network.graph.edges )
        Network.trim( graph.vertices )
        Network.trim( graph.edges )
        Network.trim( edgeList )
      }

  /***************************************************************************
   * termination routine, log then return
   ***************************************************************************/
      def terminate( loop: Long, network: Network, graph: GraphFrame ) = {
        logFile.write( "Merging terminates after " +loop.toString +" merges", false )
        logFile.close
        ( network, graph )
      }

  /***************************************************************************
   * find pair to merge according to greatest reduction in code length
   * and grab all associated quantities
   ***************************************************************************/
      def findMerge( edgeList: DataFrame ) = {
        val edge = edgeList
        // find minimum change in codelength
        // if multiple merges has the same dL, grab the one with smallest src
        .groupBy("src").min("dL")
        .orderBy("min(dL)","src")
        .limit(1)
        .alias("select").join( edgeList.alias("b"),
          col("select.min(dL)")===col("b.dL")
          && col("select.src")===col("b.src")
        )
        .select( col("b.src"), col("b.dst"), col("n1"), col("n2"),
          col("p1"), col("p2"), col("w1"), col("w2"), col("w1221"),
          col("q1"), col("q2"), col("b.dL") )
        .head
        (
          edge.getLong(0),    // src
          edge.getLong(1),    // dst
          edge.getInt(2),     // n1
          edge.getInt(3),     // n2
          edge.getDouble(4),  // p1
          edge.getDouble(5),  // p2
          edge.getDouble(6),  // w1
          edge.getDouble(7),  // w2
          edge.getDouble(8),  // w1221
          edge.getDouble(9),  // q1
          edge.getDouble(10), // q2
          edge.getDouble(11)  // dL
        )
      }

  /***************************************************************************
   * calculate newly merged module properties
   ***************************************************************************/
      def calNewcal(
        m1: Long, m2: Long, n1: Long, n2: Long,
        p1: Double, p2: Double, w1: Double, w2: Double, w1221: Double
      ) = {

        // the new modular index is the lower one
        val m12 = if( m1<m2 ) m1 else m2

        // calculate new modular properties
        val n12 = n1 +n2
        val p12 = p1 +p2
        val w12 = w1 +w2 -w1221

        val q12 = (
          network.tele *(network.nodeNumber-n12)
          /(network.nodeNumber-1) *p12
          +(1-network.tele) *w12
        )

        ( m12, n12, p12, q12, w12 )
      }

  /***************************************************************************
   * update edgeList
   * by deleting merged edge
   * updating modular properties
   * and calculating dL
   * given merged modular properties
   ***************************************************************************/
      def updateEdgeList(
        m1: Long, m2: Long, m12: Long,
        n12: Long, p12: Double, w12: Double, q12: Double,
        edgeList: DataFrame
      ): DataFrame = {
        edgeList
        // delete merged edge
        // when both src and dst hit m1 and m2
        // at least one and perhaps two edges will be deleted
        .filter(
          !( $"src"===m1 && $"dst"===m2 ) && !( $"src"===m2 && $"dst"===m1 )
        )
        // when one of src or dst hit m1 or m2,
        // update its modular property
        // and recalculate dL
        .select(
          when( col("src")===m1 || col("src")===m2, m12 )
          .otherwise(col("src"))
          as "src",
          when( col("dst")===m1 || col("dst")===m2, m12 )
          .otherwise(col("dst"))
          as "dst",
          when( col("src")===m1 || col("src")===m2, n12 )
          .otherwise(col("n1"))
          as "n1",
          when( col("dst")===m1 || col("dst")===m2, n12 )
          .otherwise(col("n2"))
          as "n2",
          when( col("src")===m1 || col("src")===m2, p12 )
          .otherwise(col("p1"))
          as "p1",
          when( col("dst")===m1 || col("dst")===m2, p12 )
          .otherwise(col("p2"))
          as "p2",
          when( col("src")===m1 || col("src")===m2, w12 )
          .otherwise(col("w1"))
          as "w1",
          when( col("dst")===m1 || col("dst")===m2, w12 )
          .otherwise(col("w2"))
          as "w2",
          col("w1221") as "w1221",
          when( col("src")===m1 || col("src")===m2, q12 )
          .otherwise(col("q1"))
          as "q1",
          when( col("dst")===m1 || col("dst")===m2, q12 )
          .otherwise(col("q2"))
          as "q2",
          col("dL") as "dL"
        )
        // aggregate edge exit probabilities
        .groupBy("src","dst").sum("w1221")
        // calculate dL
        .alias("agg")
        .join( edgeList.alias("edge"),
          col("agg.src")===col("edge.src") && col("agg.dst")===col("edge.dst") )
        .select(
          col("edge.src"), col("edge.dst"),
          col("n1"), col("n2"),
          col("p1"), col("p2"),
          col("w1"), col("w2"), col("sum(w1221)") as "w1221",
          col("q1"), col("q2"),
          CommunityDetection.calDeltaL()(
            lit(network.nodeNumber), col("n1"), col("n2"),
            col("p1"), col("p2"),
            lit(network.tele), lit(qi_sum), col("q1"), col("q2"),
            col("w1") +col("w2") -col("w1221"),
            lit( network.probSum ), lit( network.codelength )
          ) as "dL"
        )
      }

  /***************************************************************************
   * network object is actually not needed for InfoMap recursive calculation
   ***************************************************************************/
      def calNewNetwork(
        mMod: Long, mDel: Long, n12: Long, p12: Double,
        w12: Double, q12: Double,
        network: Network, newEdgeList: DataFrame, newCodelength: Double
      ): Network = {

        val newModules = network.graph.vertices
        // delete module
        .filter( s"id!=$mDel" )
        // put in new modular properties
        .select( col("id") as "id",
          when( col("id")===mMod, n12 ).otherwise("size") as "size",
          when( col("id")===mMod, p12 ).otherwise("prob") as "prob",
          when( col("id")===mMod, w12 ).otherwise("exitw") as "exitw",
          when( col("id")===mMod, q12 ).otherwise("exitq") as "exitq"
        )

        val newEdges = newEdgeList.select("src","dst","w1221")

        Network(
          network.tele, network.nodeNumber,
          GraphFrame( newModules, newEdges ),
          network.probSum, newCodelength
        )
      }

    recursiveMerge( 0, network, graph, qi_sum, edgeList )
  }

  /***************************************************************************
   * generate edgeList
   * | src , dst , n1 , n2 , p1 , p2 , w1 , w2 , w1221 , q1 , q2 , dL |
   * where n <=> size
   *       p <=> prob
   *       w <=> exitw
   *   w1221 <=> prob. going idx1->idx2 +prob. going idx2->idx1
   *       q <=> exitq
   *      dL <=> change in codelength
   ***************************************************************************/
  def generateEdgeList( network: Network, qi_sum: Double ): DataFrame = {
    network.graph.edges.alias("e1")
    // get all modular properties from "src" and "dst"
    .join( network.graph.vertices.alias("m1"), col("src") === col("m1.id") )
    .join( network.graph.vertices.alias("m2"), col("dst") === col("m2.id") )
    // find the opposite edge, needed for exitw calculation
    .join( network.graph.edges.alias("e2"),
      col("e1.src")===col("e2.dst") && col("e1.dst")===col("e2.src"),
      "left_outer" )
    // list ni, pi, wi, for i=1,2
    // and calculate w12
    .select(
      col("e1.src") as "src", col("e1.dst") as "dst",
      col("m1.size") as "n1", col("m2.size") as "n2",
      col("m1.prob") as "p1", col("m2.prob") as "p2",
      col("m1.exitw") as "w1", col("m2.exitw") as "w2",
      // prob. going between m1 and m2
      // needed for calculation of w12, the exitw of merged module of m1,m2
      // w1221 is needed because subsequent exit probability calculations
      // need to aggregate w1221's
      when( col("e2.exitw").isNotNull, col("e1.exitw") +col("e2.exitw") )
      .otherwise( col("e1.exitw") ) as "w1221"
    )
    // calculate q1, q2
    .select(
      col("src"), col("dst"),
      col("n1"), col("n2"),
      col("p1"), col("p2"),
      col("w1"), col("w2"), col("w1221"),
      CommunityDetection.calQ()(
        lit(network.tele), lit(network.nodeNumber),
        col("n1"), col("p1"), col("w1")
      ) as "q1",
      CommunityDetection.calQ()(
        lit(network.tele), lit(network.nodeNumber),
        col("n2"), col("p2"), col("w2")
      ) as "q2"
    )
    // calculate dL
    .select(
      col("src"), col("dst"),
      col("n1"), col("n2"),
      col("p1"), col("p2"),
      col("w1"), col("w2"), col("w1221"),
      col("q1"), col("q2"),
      CommunityDetection.calDeltaL()(
        lit(network.nodeNumber), col("n1"), col("n2"),
        col("p1"), col("p2"),
        lit(network.tele), lit(qi_sum), col("q1"), col("q2"),
        col("w1") +col("w2") -col("w1221"),
        lit( network.probSum ), lit( network.codelength )
      ) as "dL"
    )
  }
}
