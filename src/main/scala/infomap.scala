  /***************************************************************************
   * originaL InfoMap algorithm
   * the function to partition of nodes into modules based on
   * greedily merging the pair of modules that gives
   * the greatest code length reduction
   * until code length is minimized
   ***************************************************************************/

sealed class InfoMap extends MergeAlgo
{
  def apply( network: Network, graph: GraphFrame, logFile: LogFile ):
  ( Network, GraphFrame ) = {

    // sum of q's
    // used for deltaL calculations
    // will only be calculated once via full calculation
    // subsequent calculations will use dynamic programming
    val qi_sum = network.vertices.groupBy.sum('exitq)

    // the table that lists all possible merges,
    // the modular properties of the two vertices
    // and change in code length based on the hypothetical modular merge
    // | idx1 , idx2 , n1 , n2 , p1 , p2 , w1 , w2 , w12 , q1 , q2 , dL |
    // where n = size
    //       p = prob
    //       w = exitw
    //       q = exitq
    //      dL = change in codelength
    val edgeList = generateEdgeList( network, qi_sum )

  /***************************************************************************
   * greedily merge modules until code length is minimized
   ***************************************************************************/
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Long,
      network: Network, graph: GraphFrame
      qi_sum: Double,
      edgeList: DataFrame
    ): Partition = {

      //  create local checkpoint to truncate RDD lineage (every ten loops)
      if( loop%10 == 0 ) trim( network, graph )

      // log current state
      logFile.write( "State " +loop.toString+": "
        +"code length " +network.codelength.toString +"\n" )
      logFile.save( network, graph, "net"+loop.toString, true )

      // if there are no modules to merge, teminate
      if( table.count == 0 )
        terminate( loop, network, graph )
      else {
        val(m1,m2,n1,n2,p1,p2,w1,w2,w1_2,q1,q2,dL) = findMerge(edgeList)
        val(m12,n12,p12,w12,q12) =
          calNewcal(m1,m2,n1,n2,p1,p2,w1,w2,w1_2)
        val new_qi_sum = qi_sum +q12 -q1 -q2
        val deltaL = InfoMap.calDeltaL( deltaLi12, qi_sum, q1, q2, q12)
        val newCodelength = network.codelength +deltaL
        ///////////////////////          ISSUE HERE (qi_sum or new_qi_sum?)

        if( deltaL == 0 ) // iff the entire graph is merged into one module
        {
          if( network.codeLength < -network.ergodicFreqSum )
            terminate( loop, network, graph )
          else {
            graph.select('idx,'name,1) // all nodes are module 1
            terminate( loop, network, graph )
          }
        }
        else if( deltaL > 0 )
          terminate( loop, network, graph )
        else { // otherwise actually merge
          // log merging details
          logFile.write(
            "Merge " +loop.toString
            +": merging modules " +m1.toString +" and " +m2.toString
            +" with code length reduction " +deltaL.toString +"\n" )

          // register partition to lower merge index
          val newGraph = graph.select( 'idx, 'name
            when( 'module===m1 || 'module===m2, m12 ).otherwise('module)
          )
          val newEdgeList = genEdgeList(edgeList)
          val newNetwork = {
            val mMod = m12
            val mDel = if( m1==mMod ) m2 else m1
            calNewNetwork(
              mMod, mDel, n12, p12, w12, q12,
              network, newEdgeList
            )
          }
        }
      }
  /***************************************************************************
   * routine finished, below are function definitions and tail recursive call
   ***************************************************************************/

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
        logFile.write( "Merging terminates after " +loop.toString +" merges" )
        logFile.close
        ( network, graph )
      }

  /***************************************************************************
   * generate edgeList
   * | idx1 , idx2 , n1 , n2 , p1 , p2 , w1 , w2 , w12 , q1 , q2 , dL |
   * where n <= size
   *       p <=> prob
   *       w <=> exitw
   *       q <=> exitq
   *      dL <=> change in codelength
   ***************************************************************************/
      def generateEdgeList( network: Network, qi_sum: Double ): DataFrame = {
        network.graph.edges.alias("e1")
        // get all modular properties from "idx1" and "idx2"
        .join( network.graph.vertices.alias('m1), 'idx1 === col("m1.idx") )
        .join( network.graph.vertices.alias('m2), 'idx2 === col("m2.idx") )
        // find the opposite edge, needed for exitw calculation
        .join( network.graph.vertices.alias("e2"),
          col("e1.idx1")===col("e2.idx2") && col("e1.idx2")===col("e2.idx1"),
          "left_outer" )
        // list ni, pi, wi, for i=1,2
        // and calculate w12
        .select(
          'idx1, 'idx2,
          col("m1.size") as "n1", col("m2.size") as "n2",
          col("m1.prob") as "p1", col("m2.prob") as "p2",
          col("m1.exitw") as "w1", col("m2.exitw") as "w2",
          // calculation of the exit probability
          // of hypothetically merged module between m1 and m2
          // = w1 + w2 -w(m1->m2) -w(m2->m1)
          when( col("e2.exitw").isNotNull,
            col("m1.exitw")+col("m2.exitw")-col("e1.exitw")-col("e2.exitw")
          ).otherwise(
            col("m1.exitw")+col("m2.exitw")-col("e1.exitw")
          ) as "w12"
        )
        // calculate q1, q2
        .select( 'idx1, 'idx2, 'n1, 'n2, 'p1, 'p2, 'w1, 'w2, 'w1_2,
          calQ( network.tele, network.nodeNumber, n1, p1, w1 ) as "q1",
          calQ( network.tele, network.nodeNumber, n2, p2, w2 ) as "q2"
        )
        // calculate dL
        .select( 'idx1, 'idx2, 'n1, 'n2, 'p1, 'p2, 'w1, 'w2, 'w1_2, 'q1, 'q2,
          CommunityDetection.calDeltaL(
            network.nodeNumber, n1, n2, p1, p2,
            network.tele, qi_sum, 'q1, 'q2,
            'w_12
          ) as "dL"
        )

        // simple wrapper to calculate Q as a Spark SQL function
        def calQ(
          tele: Column
          nodeNumber: Column, size: Column, prob: Column,
          exitw: Column
        ): Column = CommunityDetection.calQ(
          tele, nodeNumber, size, prob, exitw
        )
      }

  /***************************************************************************
   * find pair to merge according to greatest reduction in code length
   * and grab all associated quantities
   ***************************************************************************/
      def findMerge( edgeList: DataFrame ) = {
        edgeList
        // find minimum change in codelength
        .groupBy.min('dL)
        // if multiple merges has the same dL, grab the one with smallest idx1
        .join( edgeList, 'dL )
        .groupBy.min('idx1)
        .alias("select").join( edgeList.alias("b"),
          col("select.idx1")===col("b.idx1") && col("select.dL")===col("b.dL")
        )
        .select('idx1,'idx2,'n1,'n2,'p1,'p2,'w1,'w2,'w1_2,'q1,'q2,'dL)
        .head
        match {
          case Row(m1,m2,n1,n2,p1,p2,w1,w2,w12,q1,q2,dL)
          => (m1,m2,n1,n2,p1,p2,w1,w2,w12,q1,q2,dL)
        }
      }

  /***************************************************************************
   * calculate newly merged module properties
   ***************************************************************************/
      def calNewcal(
        m1: Long, m2: Long, n1: Long, n2: Long,
        p1: Double, p2: Double, w1: Double, w2: Double, w1_2: Double
      ) = {

        // the new modular index is the lower one
        val m12 = if( m1<m2 ) m1 else m2

        // calculate new modular properties
        val n12 = n1 +n2
        val p12 = p1 +p2

        // w12 is the exitw of module m12
        // w1_2 is the sum of exitw of edges (idx1,idx2) and (idx2,idx1)

        // also needs w2_1 to calculate w12
        val w2_1 = edgeList.filter( 'idx1===m2 && 'idx2===m1 )
        .select('w1_2)
        .head.getDouble(0)
        val w12 = w1 +w2 -w1_2 -w2_1
        val q12 = CommunityDetection.calQ(
          network.tele, network.nodeNumber, n12, p12, tele, w12 )

        ( m12, n12, p12, w12, q12 )
      }

  /***************************************************************************
   * update edgeList
   * by deleting merged edge
   * updating modular properties
   * and calculating dL
   * given merged modular properties
   ***************************************************************************/
      def genEdgeList(
        m1: Long, m2: Long, m12: Long,
        n12: Long, p12: Double, w12: Double, w1_2: Double, q12: Double,
        edgeList: DataFrame
      ): DataFrame = {
        edgeList
        // delete merged edge
        // when both idx1 and idx2 hit m1 and m2
        // at least one and perhaps two edges will be deleted
        .filter(
          !( 'idx1===m1 && 'idx2===m2 ) &&
          !( 'idx1===m2 && 'idx2===m1 )
        )
        // when one of idx1 or idx2 hit m1 or m2,
        // update its modular property
        // and recalculate w12
        .select(
          when( 'idx1===m1 || 'idx1===m2, m12 ).otherwise('idx1),
          when( 'idx2===m1 || 'idx2===m2, m12 ).otherwise('idx2),
          when( 'idx1===m1 || 'idx1===m2, n12 ).otherwise('n1),
          when( 'idx2===m1 || 'idx2===m2, n12 ).otherwise('n2),
          when( 'idx1===m1 || 'idx1===m2, p12 ).otherwise('p1),
          when( 'idx2===m1 || 'idx2===m2, p12 ).otherwise('p2),
          when( 'idx1===m1 || 'idx1===m2, w12 ).otherwise('w1),
          when( 'idx2===m1 || 'idx2===m2, w12 ).otherwise('w2),
          'w_12,
          when( 'idx1===m1 || 'idx1===m2, q12 ).otherwise('q1),
          when( 'idx2===m1 || 'idx2===m2, q12 ).otherwise('q2)
        )
        // aggregate edge weights
        .groupBy('idx1,'idx2).sum('w1_2)
      }

  /***************************************************************************
   * network object is actually not needed for InfoMap recursive calculation
   ***************************************************************************/
      def calNewNetwork(
        mMod: Long, mDel: Long, n12: Long, p12: Long, w12: Double, q12: Double,
        network: Network, newEdgeList: DataFrame
      ): Network = {

        val newModules = network.graph.vertices
        // delete module
        .filter('idx!=mDel)
        // put in new modular properties
        .select( 'idx,
          when( 'idx===mMod, n12 ).otherwise('size),
          when( 'idx===mMod, p12 ).otherwise('prob),
          when( 'idx===mMod, w12 ).otherwise('exitw),
          when( 'idx===mMod, q12 ).otherwise('exitq)
        )

        val newEdges = newEdgeList.select('idx1,'idx2,'w1_2)

        Network(
          network.nodeNumber, network.tele,
          newModules, newEdges,
          newCodeLength
        )
      }

  /***************************************************************************
   * recursive call
   ***************************************************************************/
      recursiveMerge( loop+1, newNetwork, newGraph, new_qi_sum, newEdgeList )
    }
    recursiveMerge( 0, network, graph, qi_sum, edgeList )
  }
}
