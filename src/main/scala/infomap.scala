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
    val qi_sum = network.graph.vertices.groupBy().sum("exitq").head.getDouble(0)

    // the table that lists all possible merges,
    // the modular properties of the two vertices
    // and change in code length based on the hypothetical modular merge
    // | idx1 , idx2 , n1 , n2 , p1 , p2 , w1 , w2 , w1221 , q1 , q2 , dL |
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
      if( edgeList.groupBy().count.head.getLong(0) == 0 )
        terminate( loop, network, graph )
      else {
        val (m1,m2,n1,n2,p1,p2,w1,w2,w1221,q1,q2,deltaL) = findMerge(edgeList)
        val (m12,n12,p12,w12,q12) = calNewcal(m1,m2,n1,n2,p1,p2,w1,w2,w1221)

        if( deltaL == 0 ) // iff the entire graph is merged into one module
        {
          if( network.codelength < -network.probSum )
            terminate( loop, network, graph )
          else {
            val newVertices = network.graph.vertices
            .groupBy().count
            .select( lit(1) as "idx", lit(network.nodeNumber) as "size",
              lit(1) as "prob", lit(0) as "exitw", lit(0) as "exitq" )
            val newEdges = network.graph.edges.filter("false")
            val newNetwork = Network(
              network.tele, network.nodeNumber,
              GraphFrame( newVertices, newEdges ),
              network.probSum, -network.probSum
            )
            terminate( loop, newNetwork, GraphFrame(newVertices,newEdges) )
          }
        }
        else if( deltaL > 0 )
          terminate( loop, network, graph )
        else { // merge two modules
          // log merging details
          logFile.write(
            "Merge " +loop.toString
            +": merging modules " +m1.toString +" and " +m2.toString
            +" with code length reduction " +deltaL.toString +"\n", false )

          // register partition to lower merge index
          val newGraph = GraphFrame( graph.vertices.select( col("idx"), col("name"),
            when( col("module")===m1 || col("module")===m2, m12 ).otherwise("module") ),
            graph.edges
          )
          val newEdgeList = updateEdgeList(m1,m2,m12,n12,p12,w12,q12,edgeList)
          val newCodelength = network.codelength +deltaL
          val newNetwork = {
            val mMod = m12
            val mDel = if( m1==mMod ) m2 else m1
            calNewNetwork(
              mMod, mDel, n12, p12, w1221, q12,
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
        edgeList
        // find minimum change in codelength
        .groupBy().min("dL")
        // if multiple merges has the same dL, grab the one with smallest idx1
        .join( edgeList, "dL" )
        .groupBy().min("idx1")
        .alias("select").join( edgeList.alias("b"),
          col("select.idx1")===col("b.idx1") && col("select.dL")===col("b.dL")
        )
        .select("idx1","idx2","n1","n2","p1","p2","w1","w2","w1221","q1","q2","dL")
        .head
        match {
          case Row(m1:Long,m2:Long,n1:Long,n2:Long,p1:Double,p2:Double,w1:Double,w2:Double,w1221:Double,q1:Double,q2:Double,dL:Double)
          => (m1,m2,n1,n2,p1,p2,w1,w2,w1221,q1,q2,dL)
        }
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

        val q12 = CommunityDetection.calQ_(
          network.tele, network.nodeNumber, n12, p12, w12 )

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
        // when both idx1 and idx2 hit m1 and m2
        // at least one and perhaps two edges will be deleted
        .filter(
          "!( idx1===m1 && idx2===m2 ) && !( idx1===m2 && idx2===m1 )"
        )
        // when one of idx1 or idx2 hit m1 or m2,
        // update its modular property
        // and recalculate dL
        .select(
          when( col("idx1")===m1 || col("idx1")===m2, m12 ).otherwise(col("idx1")),
          when( col("idx2")===m1 || col("idx2")===m2, m12 ).otherwise(col("idx2")),
          when( col("idx1")===m1 || col("idx1")===m2, n12 ).otherwise(col("n1")),
          when( col("idx2")===m1 || col("idx2")===m2, n12 ).otherwise(col("n2")),
          when( col("idx1")===m1 || col("idx1")===m2, p12 ).otherwise(col("p1")),
          when( col("idx2")===m1 || col("idx2")===m2, p12 ).otherwise(col("p2")),
          when( col("idx1")===m1 || col("idx1")===m2, w12 ).otherwise(col("w1")),
          when( col("idx2")===m1 || col("idx2")===m2, w12 ).otherwise(col("w2")),
          col("w1221"),
          when( col("idx1")===m1 || col("idx1")===m2, q12 ).otherwise(col("q1")),
          when( col("idx2")===m1 || col("idx2")===m2, q12 ).otherwise(col("q2")),
          col("dL")
        )
        // aggregate edge exit probabilities
        .groupBy("idx1","idx2").sum("w1221")
        // calculate dL
        .select(col("idx1"),col("idx2"),col("n1"),col("n2"),col("p1"),col("p2"),col("w1"),col("w2"),col("w1221"),col("q1"),col("q2"),
          CommunityDetection.calDeltaL()(
            lit(network.nodeNumber), col("n1"), col("n2"), col("p1"), col("p2"),
            lit(network.tele), lit(qi_sum), col("q1"), col("q2"), (col("w1")+col("w2")-col("w1221"))
          )
        )
      }

  /***************************************************************************
   * network object is actually not needed for InfoMap recursive calculation
   ***************************************************************************/
      def calNewNetwork(
        mMod: Long, mDel: Long, n12: Long, p12: Double,
        w1221: Double, q12: Double,
        network: Network, newEdgeList: DataFrame, newCodelength: Double
      ): Network = {

        val newModules = network.graph.vertices
        // delete module
        .filter("idx!=mDel")
        // put in new modular properties
        .select( col("idx"),
          when( col("idx")===mMod, col("n12") ).otherwise("size"),
          when( col("idx")===mMod, col("p12") ).otherwise("prob"),
          when( col("idx")===mMod, col("w12") ).otherwise("exitw"),
          when( col("idx")===mMod, col("q12") ).otherwise("exitq")
        )

        val newEdges = newEdgeList.select("idx1","idx2","w1221")

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
   * | idx1 , idx2 , n1 , n2 , p1 , p2 , w1 , w2 , w1221 , q1 , q2 , dL |
   * where n <=> size
   *       p <=> prob
   *       w <=> exitw
   *   w1221 <=> prob. going idx1->idx2 +prob. going idx2->idx1
   *       q <=> exitq
   *      dL <=> change in codelength
   ***************************************************************************/
  def generateEdgeList( network: Network, qi_sum: Double ): DataFrame = {
    network.graph.edges.alias("e1")
    // get all modular properties from "idx1" and "idx2"
    .join( network.graph.vertices.alias("m1"), col("idx1") === col("m1.idx") )
    .join( network.graph.vertices.alias("m2"), col("idx2") === col("m2.idx") )
    // find the opposite edge, needed for exitw calculation
    .join( network.graph.vertices.alias("e2"),
      col("e1.idx1")===col("e2.idx2") && col("e1.idx2")===col("e2.idx1"),
      "left_outer" )
    // list ni, pi, wi, for i=1,2
    // and calculate w12
    .select(
      col("idx1"), col("idx2"),
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
    .select( col("idx1"), col("idx2"), col("n1"), col("n2"), col("p1"), col("p2"), col("w1"), col("w2"), col("w1221"),
      CommunityDetection.calQ()( lit(network.tele), lit(network.nodeNumber), col("n1"), col("p1"), col("w1") ) as "q1",
      CommunityDetection.calQ()( lit(network.tele), lit(network.nodeNumber), col("n2"), col("p2"), col("w2") ) as "q2"
    )
    // calculate dL
    .select( col("idx1"), col("idx2"), col("n1"), col("n2"), col("p1"), col("p2"), col("w1"), col("w2"), col("w1221"), col("q1"), col("q2"),
      CommunityDetection.calDeltaL()(
        lit(network.nodeNumber), col("n1"), col("n2"), col("p1"), col("p2"),
        lit(network.tele), lit(qi_sum), col("q1"), col("q2"), (col("w1") +col("w2") -col("w1221"))
      ) as "dL"
    )
  }
}
