  /***************************************************************************
   * InfoFlow community detection algorithm
   *
   * this is the multimerging algorithm
   * where each module merges with another module
   * that gives the greatest reduction in the code length
   * so that the following may happen for a module:
   *   (1) it seeks no merge, because of no connections with other modules,
   *       or all potential merges increase code length
   *   (2) it seeks a merge with another module (which also seeks a merge w/ it)
   *   (3) it seeks a merge with another, which seeks merge with some other
   *       module, and so on; in which case we merge all these modules
   *
   * this is a big file, organized so that:
   * it contains a simple class with only one function, apply()
   * which contains only a tail recursive function
   * this tail recursive function contains many calculated quantities
   * often one quantity's calculation involves the other, forming a DAG
   * the calculations are organized so that
   * each each calculation is defined within a function
   * and called in the main routine
   ***************************************************************************/

import org.apache.spark.sql._
import org.graphframes._

sealed class InfoFlow extends CommunityDetection
{
  def apply( network: Network, graph: GraphFrame, logFile: LogFile ):
  ( Network, GraphFrame ) = {
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Long, network: Network, graph: GraphFrame
    ): ( Network, GraphFrame ) = {

  /***************************************************************************
   * main routine, contains calculation functions defined subsequently
   ***************************************************************************/

      // log current state
      logFile.write( "State " +loop.toString+": "
        +"code length " +network.codelength.toString +"\n", false )
      logFile.save( network.graph, graph, true, "net"+loop.toString )

      // truncate RDD lineage every ten loops
      if( loop%10 == 0 ) {
        Network.trim( network.graph.vertices )
        Network.trim( network.graph.edges )
        Network.trim( graph.vertices )
        Network.trim( graph.edges )
      }

      // calculate the deltaL table for all possible merges
      // | idx1 , idx2 , dL |
      val deltaL = calDeltaL(network)

      // module to merge
      // |module , module to seek merge to |
      val m2Merge = calm2Merge(deltaL)

      // if m2Merge is empty, then no modules seek to merge
      // terminate loop
      if( m2Merge.count == 0 )
        terminate( loop, network, graph )

      // map each network.vertices to a new module
      // according to connected components of m2Merge
      // | idx , module |
      val moduleMap = calModuleMap( network, m2Merge )

      // new nodal-modular partitioning scheme
      // | idx , module |
      // difference from moduleMap:
      // (1) moduleMap verticesare modules
      //     newPartition nodes are all original nodes
      // (2) moduleMap used only within this function for further calculations
      //     newPartition saved to graph, which is part of final result
      val newGraphVertices = calNewPartition( moduleMap, graph.vertices )
      // update graph partitioning
      val newGraph = GraphFrame( newGraphVertices, graph.edges )

      // intermediate edges
      // map the associated modules into new module indices
      // if the new indices are the same, they are intramodular connections
      // and will be subtracted from the w_i's
      // if the new indices are different, they are intermodular connections
      // and will be aggregated into w_ij's
      // | idx1 , idx2 , iWj |
      val interEdges = calInterEdges( network, moduleMap )
      // cache result, which is to calculate both newModules and newEdges
      .cache

      val newModules = calNewModules( network, moduleMap, interEdges )
      val newNodeNumber = newModules.groupBy().count

      // calculate new code length, and terminate if bigger than old
      val newCodelength = CommunityDetection.calCodelength(
        newModules, network.probSum )
      if( newCodelength >= network.codelength )
        terminate( loop, network, graph )

      // logging: merge details
      logFile.write( "Merge " +(loop+1).toString
        +": merging " +network.graph.vertices.count.toString
        +" modules into " +newModules.count.toString +" modules\n",
        false
      )

      // calculate new network
      val newEdges = interEdges.filter( 'idx1 != 'idx2 )
      val newNetwork = calNewNetwork(
        network,
        newCodelength,
        newModules, newEdges
      )

      // routine finished after this, tail recursive function call next
      // with calculation function definitions below

  /***************************************************************************
   * termination routine, log then return
   ***************************************************************************/
      def terminate( loop: Long, network: Network, graph: GraphFrame ) = {
        logFile.write(
          "Merging terminates after " +loop.toString +" merges\n",
          false )
        logFile.close
        ( network, graph )
      }

  /***************************************************************************
   * table of all possible merges, and code length change
   * | idx1 , idx2 , dL |
   ***************************************************************************/
      def calDeltaL( network: Network ): DataFrame = {
        val qi_sum = network.graph.vertices.groupBy().sum('exitq)
        network.graph.edges.alias("e1")
        // get all modular properties from "idx1" and "idx2"
        .join( network.graph.vertices.alias('m1), 'idx1 === col("m1.idx") )
        .join( network.graph.vertices.alias('m2), 'idx2 === col("m2.idx") )
        // find the opposite edge, needed for exitw calculation
        .join( network.graph.vertices.alias("e2"),
          col("e1.idx1")===col("e2.idx2") && col("e1.idx2")===col("e2.idx1"),
          "left_outer" )
        // create table of change in code length of all possible merges
        .select(
          'idx1, 'idx2,
          CommunityDetection.calDeltaL(
            network.nodeNumber, "m1.size", "m2.size", "m1.prob", "m2.prob",
            network.tele, qi_sum, "m1.exitq", "m2.exitq",
            // calculation of the exit probability
            // of hypothetically merged module between m1 and m2
            // = w1 + w2 -w(m1->m2) -w(m2->m1)
            when( col("e2.exitw").isNotNull,
              col("m1.exitw")+col("m2.exitw")-col("e1.exitw")-col("e2.exitw")
            ).otherwise(
              col("m1.exitw")+col("m2.exitw")-col("e1.exitw")
            )
          ) as "dL"
        )
      }
      // importantly, dL is symmetric towards idx1 and idx2
      // so if both edges (idx1,idx2) and (idx2,idx1) exists
      // their dL would be identical
      // since dL (and the whole purpose of this table)
      // is used to decide on merge, there is an option
      // on whether a module could seek merge with another
      // if there is an opposite connection
      // eg a graph like this: m0 <- m1 -> m2 -> m3
      // m2 seeks to merge with m3
      // m1 might merge with m0
      // BUT the code length reduction if m2 seeks merge with m1
      // is greater than that of m2 seeking merge with m3
      // the question arise, should such a merge (opposite direction to edge),
      // be considered?
      // this dilemma stems from how edges are directional
      // while merges are non-directional, symmetric towards two modules
      // in the bigger picture, this merge seeking behaviour
      // is part of a greedy algorithm
      // so that the best choice is heuristic based only
      // to keep things simple, don't consider opposite edge merge now

  /***************************************************************************
   * each module seeks to merge with another connected module
   * which would offer the greatest reduction in code length
   * this forms (weakly) connected components of merged modules
   * to be generated later
   *
   * this implementation has two subtleties:
   *   (1) merge seeks are directional
   *       so that m2 will never seek to merge with m1 if only (m1,m2) exists
   *   (2) (m1,m2), (m1,m3) has identical code length reduction
   *       then both merges are selected
   *       that is, m1 seeks to merge with both m2 and m3
   * |module , module to seek merge to |
   ***************************************************************************/
      def calm2Merge( deltaL: DataFrame ): DataFrame = {
        // the most favoured merge based on code length reduction
        val bestMergeFrom1 = deltaL
        .groupBy('idx1).min('dL)
        // filter away rows with non-negative dL
        .filter( 'dL >= 0 )
        // join with deltaL to retrieve idx2
        .alias("bm")
        .join( deltaL,
          col("bm.idx1")===col("deltaL.idx1") && "deltaL.dL"==="bm.dL"
        )
        .select( 'idx1, 'idx2 )
      }

  /***************************************************************************
   * map each network.vertices to a new module
   * according to connected components of m2Merge
   * | idx , module |
   ***************************************************************************/
      def calModuleMap( network: Network, m2Merge: DataFrame ): DataFrame = {
        GraphFrame( network.nodes, m2Merge )
        .connectedComponents.run
        .vertices.select( 'idx, 'components as "module" )
      }

  /***************************************************************************
   * calculatenew nodal-modular partitioning scheme
   * difference from moduleMap:
   *   (1) moduleMap verticesare modules
   *       newPartition nodes are all original nodes
   *   (2) moduleMap used only within this function for further calculations
   *       newPartition saved to graph, which is part of final result
   * | idx , module |
   ***************************************************************************/
      def calNewPartition( moduleMap: DataFrame, graphVertices: DataFrame ):
      DataFrame = {
        graphVertices.alias("original")
        .join( moduleMap.alias("new"), 'idx, "left_outer" )
        .select( 'node,
          when( 'idx.isNotNull, "new.module" )
         .otherwise( "original.module" )
        )
      }

  /***************************************************************************
   * intermediate edges
   * map the associated modules into new module indices
   * if the new indices are the same, they are intramodular connections
   * and will be subtracted from the w_i's
   * if the new indices are different, they are intermodular connections
   * and will be aggregated into w_ij's
   * | idx1 , idx2 , iWj |
   ***************************************************************************/
      def calInterEdges(
        network: Network, moduleMap: DataFrame
      ): DataFrame = {
        network.edges
        // map vertex indices to new ones
        .join( moduleMap, "idx1" === "idx" )
        .select( 'module as "m1", 'idx2, 'weight )
        .join( moduleMap, "idx2" === "idx" )
        .select( 'm1, 'module as "m2", 'weight )
        .select( 'm1 as "idx1", 'm2 as "idx2", 'weight )
        // aggregate
        .groupBy('idx1,'idx2)
        .sum('weight)
      }

  /***************************************************************************
   * modular properties calculations
   ***************************************************************************/
      def calNewModules(
        network: Network, moduleMap: DataFrame, interEdges: DataFrame
      ): DataFrame = {
        // aggregate size, prob, exitw over the same modular index
        // for size and prob, that gives the final result
        // for exitw, we have to subtract intramodular edges in the next step
        val sumOnly = network.graph.vertices
        // map to new module index
        .join( moduleMap, 'idx )
        // aggregate
        .groupBy('module).sum( "size", "prob", "exitw" )
        // name properly
        .select('module as "idx", 'size, 'prob, 'exitw )

        // subtract intramodular edges from modular exit prob
        val intraEdges = interEdges
        // intramodular edges have same indices
        .filter( 'idx1 === 'idx2 )
        // aggregate
        .groupBy('idx1).sum('exitw)
        // name properly
        .select( 'idx1 as "idx", 'exitw )

        sumOnly.alias("m")
        .join( intraEdges.aslias("e"), 'idx, "outer_left" )
        .select( 'idx, 'size, 'prob,
          // calculation of exitw
          // subtract intramodular edges from modular exit prob
          // if former exists
          when( col("e.exitw").isNotNull, col("m.exitw") -col("m.exitw") )
          .otherwise( col("m.exitw") ) as "exitw"
        )
        .select( 'idx, 'size, 'prob, 'exitw,
          // calculation of exitq
          // subtract intramodular edges from modular exit prob
          // if former exists
          calQ( network.tele, network.nodeNumber, 'size, 'prob, 'exitw )
          as "exitq"
        )

        // simple wrapper to calculate Q as a Spark SQL function
        def calQ(
          tele: Column,
          nodeNumber: Column, size: Column, prob: Column,
          exitw: Column
        ): Column = CommunityDetection.calQ(
          tele, nodeNumber, size, prob, exitw
        )
      }

  /***************************************************************************
   * generate new graph and new network
   ***************************************************************************/

      def calNewNetwork(
        network: Network, newCodeLength: Double,
        newModules: DataFrame, newEdges: DataFrame
      ): Network = {
        val newGraph = GraphFrame( newModules, newEdges )
        Network(
          network.tele, newNodeNumber,
          newGraph,
          network.probSum, newCodeLength
        )
      }

  /***************************************************************************
   * end of calculation functions
   * invoke recursive calls
   ***************************************************************************/
      recursiveMerge( loop+1, newNetwork, newGraph )
    }
    recursiveMerge( 0, network, graph )
  }
}
