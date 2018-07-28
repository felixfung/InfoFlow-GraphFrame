sealed class InfoFlow extends MergeAlgo
{
  def apply( network: Network, logFile: LogFile ): ( Network, DataFrame ) = {

  /***************************************************************************
   * this is the multimerging algorithm
   * where each module merges with another module
   * that gives the greatest reduction in the code length
   * so that the following may happen for a module:
   *   (1) it seeks no merge, because of no connections with other modules,
   *       or all potential merges increase code length
   *   (2) it seeks a merge with another module (which also seeks a merge w/ it)
   *   (3) it seeks a merge with another, which seeks merge with some other
   *       module, and so on; in which case we merge all these modules
   ***************************************************************************/
    def recursiveMerge(
      loop: Long, network: Network, partition: DataFrame
    ): ( Network, DataFrame ) = {

  /***************************************************************************
   * termination routine, log then return
   ***************************************************************************/
      private def terminate = {
        logFile.write( "Merging terminates after " +loop.toString +" merges" )
        logFile.close
        ( network, partition )
      }

  /***************************************************************************
   * create local checkpoint to truncate RDD lineage (every ten loops)
   ***************************************************************************/
      if( loop%10 == 0 )
        network.localCheckpoint

  /***************************************************************************
   * calculate the deltaL table for all possible merges
   ***************************************************************************/

      // table of all possible merges, and code length change
      // | idx1 , idx2 , dL |
      val deltaL = {
        val qi_sum = network.groupBy.sum("exitq")

        // udf function to calculate change in code length
        def calDeltaL(
          nodeNumber: Long,
          n1: Long, n2: Long, p1: Double, p2: Double,
          tele: Double, w12: Double,
          qi_sum: Double, q1: Double, q2: Double
        ) = {
          val(_,deltaL) = MergeAlgo.calDeltaL(
            nodeNumber,
            n1, n2, p1, p2,
            tele, w12,
            qi_sum, q1, q2
          )
          deltaL
        }
        sqlc.udf.register( "calDeltaL", calDeltaL )

        network.graph.edges
        .join(
        // get all modular properties from "m1" and "m2"
          network.graph.vertices.alias("m1"),
          network.edges("idx1") === network.vertices("idx")
        )
        .join(
          network.graph.vertices.alias("m2"),
          network.edges("idx2") === network.vertices("idx")
        )
        // create table of change in code length of all possible merges
        .select(
          col("idx1"), col("idx2"),
          calDeltaL(
            network.nodeNumber, "m1.size", "m2.size", "m1.prob", "m2.prob",
            network.tele, "m1.exitw"+"m2.exitw"-"weight",
            qi_sum, "m1.exitq", "m2.exitq"
          ) as "dL"
        )
      }

  /***************************************************************************
   * logging: current code length
   ***************************************************************************/

      logFile.write( "State " +loop.toString+": "
        +"code length " +network.codelength.toString +"\n" )

  /***************************************************************************
   * each module seeks to merge with another connected module
   * which would offer the greatest reduction in code length
   * this forms connected components of merged modules
   * since the direction of merges are unimportant,
   * this forms an undirected graph
   * so that weakly and strongly connected components are same
   ***************************************************************************/
      // module to merge
      // |module , module to seek merge to|
      val m2Merge = {
        // the most favoured merge from idx1
        val bestMergeFrom1 = deltaL.groupBy("idx1").min("dL").alias("bm1")
        // the most favoured merge from idx2
        val bestMergeFrom2 = deltaL.groupBy("idx2").min("dL").alias("bm2")

        // outer join bm1 and bm2, to get [ idx, dL ]
        bestMergeFrom2.join( bestMergeFrom2, "outer" )
        .select(
          when( 'idx1.isNotNull && 'idx2.isNotNull
            && 'idx1<='idx2, 'idx1 )
         .when( 'idx1.isNotNull && 'idx2.isNotNull
            && 'idx1> 'idx2, 'idx2 )
         .when( 'idx1.isNotNull, 'idx1 )
         .when( 'idx2.isNotNull, 'idx2 )
          as "idx",
          when( 'idx1.isNotNull && 'idx2.isNotNull
            && 'idx1<='idx2, col("bm1.dL") )
         .when( 'idx1.isNotNull && 'idx2.isNotNull
            && 'idx1> 'idx2, col("bm2.dL") )
         .when( 'idx1.isNotNull, col("bm1.dL") )
         .when( 'idx2.isNotNull, col("bm2.dL") )
          as "dL"
        )
        // group by to get unique [ idx, dL ] with minmial dL
        .groupBy("idx")
        .min("dL")
        // filter away rows with non-negative dL
        .filter( "dL" >= 0 )
        // join with deltaL to obtain both vertices
        .alias("bm")
        .join( deltaL, (
            "idx"==="idx1" || "idx"==="idx2" )
          && "deltaL.dL"==="bm.dL"
        )
        .select( 'idx1, 'idx2 )
      }

      // if m2Merge is empty, then no modules seek to merge
      // terminate loop
      if( m2Merge.count == 0 )
        terminate

  /***************************************************************************
   * for all inter-modular connection, assign it to a module
   ***************************************************************************/

      // find connected components
      // | idx , module |
      val moduleMap = GraphFrame( network.nodes, m2Merge )
      .connectedComponents.run
      .node.vertices.select( 'idx, 'connectedcomponents as "module" )

      // new nodal-modular partitioning scheme
      // | idx , module |
      // difference from moduleMap:
      // moduleMap nodes are modules
      // newPartition nodes are all original nodes
      val newPartition = partition.join( moduleMap,
        'node === 'idx, "left_outer" )
      .select( 'node,
        when( 'idx.isNotNull, "moduleMap.module" )
       .otherwise( "partition.module" )
      )

  /***************************************************************************
   * intermediate edge properties calculations
   ***************************************************************************/

      // intermediate edges
      // map the associated modules into new module indices
      // if the new indices are the same, they are intramodular connections
      // and will be subtracted from the w_i's
      // if the new indices are different, they are intermodular connections
      // and will be aggregated into w_ij's
      // | module1 | module2 | iWj |
      val interEdges = network.edges
      .join( moduleMap, "idx1" === "idx" )
      .select( 'module as "m1", 'idx2, 'weight )
      .join( moduleMap, "idx2" === "idx" )
      .select( 'm1, 'module as "m2", 'weight )
      // index order may be rearranged, so smaller index comes first
      .select(
        when( 'm1 <= 'm2, 'm1 ).otherwise('m2) as "idx1",
        when( 'm1 <= 'm2, 'm2 ).otherwise('m1) as "idx2",
        'weight
      )
      // aggregate exit prob w/o tele
      .groupBy('idx1,'idx2)
      .sum('weight)
      // cache result, which is used to calculate more than one quantity later
      .cache

  /***************************************************************************
   * modular properties calculations
   ***************************************************************************/

      // | module , n, p , w , q |
      val newModules = {
        // aggregate n,p,w over the same modular index
        // for n and p, that gives the final result
        // for w, we have to subtract w12 in the next step
        val sumOnly = network.vertices
        .join( moduleMap, "idx" )
        .groupBy("idx")
        .sum( "size", "prob", "exitw" )

        // subtract w12 (intramodular exit prob) from modular exit prob
        val intraEdges = interEdges
        // intramodular edges have same indices
        .filter( 'idx1 === 'idx2 )
        .select( 'idx1 as "idx", 'weight )

        def calQ(
          nodeNumber: Long, n: Long: p: Double, tele: Double: w: Double
        ): Double =
          Partition.calQ( nodeNumber, n, p, tele, w )
        sqlc.udf.register( "calQ", calQ )

        sumOnly.join( intraEdges, 'idx, "outer_left" )
        .select( 'idx, 'size, 'prob,
          when('weight.isNotNull,'exitw-'weight).otherwise('exitw) as "exitw"
          when('weight.isNotNull,
            calQ( nodeNumber, n, p, network.tele, w-w12 )
          )
          .otherwise(
            calQ( nodeNumber, n, p, network.tele, w )
          )
          as "exitw"
        )
      }

      val newNodeNumber = newModules.groupBy.count

  /***************************************************************************
   * code length calculations
   ***************************************************************************/

      // if code length is not reduced, terminate
      val newCodeLength = calCodeLength(newModules)
      if( newCodeLength >= network.codeLength )
        terminate

  /***************************************************************************
   * generate new graph and new network
   ***************************************************************************/

      val newNetwork = {
        val newEdges = interEdges.filter( 'idx1 != 'idx2 )
        val newGraph = GraphFrame( newModules, newEdges )
        Network( network.tele, newNodeNumber, newGraph, newCodeLength )
      }

  /***************************************************************************
   * logging: merge details
   ***************************************************************************/

      logFile.write( "Merge " +(loop+1).toString
        +": merging " +network.vertices.count.toString
        +" modules into " +newModules.count.toString +" modules\n"
      )

  /***************************************************************************
   * recursive function call
   ***************************************************************************/
      recursiveMerge( loop+1, newNetwork, newPartition )
    }

    // table that maps each node to a module
    // initial partition: each node is its own module
    val partition = modules.select('idx as "node", 'idx as "module")
    recursiveMerge( 0, network, partition )
  }
}
