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
import org.apache.spark.sql.functions._

sealed class InfoFlow extends CommunityDetection
{
  def apply( network: Network, graph: GraphFrame, logFile: LogFile ):
  ( Network, GraphFrame ) = {
    @scala.annotation.tailrec
    def recursiveMerge(
      loop: Long, network: Network, graph: GraphFrame
    ): ( Network, GraphFrame ) = {

    // this is to use short-hand 'columnName to access DataFrame column
    val spark = SparkSession.builder.appName("InfoFlow").getOrCreate()
    import spark.implicits._

  /***************************************************************************
   * main routine, contains calculation functions defined subsequently
   ***************************************************************************/

      // log current state
      logFile.write( "State " +loop.toString+": "
        +"code length " +network.codelength.toString +"\n", false )
      logFile.save( network.graph, graph, true, "net"+loop.toString )

      // truncate RDD lineage every ten loops
      if( loop%10 == 0 ) trim

      // calculate the deltaL table for all possible merges
      // | src , dst , dL |
      val deltaL = calDeltaL(network)

      // module to merge
      // |module , module to seek merge to |
      val m2Merge = calm2Merge(deltaL)
      m2Merge.cache

      // if m2Merge is empty, then no modules seek to merge
      // terminate loop
      if( m2Merge.count == 0 )
        return terminate( loop, network, graph )

      // map each network.vertices to a new module
      // according to connected components of m2Merge
      // | id , module |
      val moduleMap = calModuleMap( network, m2Merge )

      // new nodal-modular partitioning scheme
      // | id , module |
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
      // | src , dst , iWj |
      val interEdges = calInterEdges( network, moduleMap )
      // cache result, which is to calculate both newModules and newEdges
      .cache

      val newModules = calNewModules( network, moduleMap, interEdges )
      val newNodeNumber = {
        val count = newModules.groupBy().count
        count.cache
        count.head.getLong(0)
      }

      // calculate new code length, and terminate if bigger than old
      val newCodelength = CommunityDetection.calCodelength(
        newModules, network.probSum )
      if( newCodelength >= network.codelength )
        return terminate( loop, network, graph )

      // logging: merge details
      logFile.write( "Merge " +(loop+1).toString
        +": merging " +network.graph.vertices.count.toString
        +" modules into " +newModules.count.toString +" modules\n",
        false
      )

      // calculate new network
      val newEdges = interEdges.filter( "src != dst" )
      val newNetwork = calNewNetwork(
        network,
        newNodeNumber, newCodelength,
        newModules, newEdges
      )

  /***************************************************************************
   * end of calculation functions
   * invoke recursive calls
   * routine finished after this, tail recursive function call next
   * with calculation function definitions below
   ***************************************************************************/
      recursiveMerge( loop+1, newNetwork, newGraph )
    }

    def trim(): Unit = {
      Network.trim( network.graph.vertices )
      Network.trim( network.graph.edges )
      Network.trim( graph.vertices )
      Network.trim( graph.edges )
    }

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
   * | src , dst , dL |
   ***************************************************************************/
    def calDeltaL( network: Network ): DataFrame = {
      val qi_sum = {
        val sum = network.graph.vertices.groupBy().sum("exitq")
        sum.cache
        sum.head.getDouble(0)
      }

      network.graph.edges.alias("e1")
      // get all modular properties from "src" and "dst"
      .join( network.graph.vertices.alias("m1"), col("src") === col("m1.id") )
      .join( network.graph.vertices.alias("m2"), col("dst") === col("m2.id") )
      // find the opposite edge, needed for exitw calculation
      .join( network.graph.edges.alias("e2"),
        col("e1.src")===col("e2.dst") && col("e1.dst")===col("e2.src"),
        "left_outer" )
      // create table of change in code length of all possible merges
      .select(
        col("e1.src"), col("e1.dst"),
        CommunityDetection.calDeltaL()(
          lit(network.nodeNumber), col("m1.size"), col("m2.size"),
          col("m1.prob"), col("m2.prob"),
          lit(network.tele), lit(qi_sum), col("m1.exitq"), col("m2.exitq"),
          // calculation of the exit probability
          // of hypothetically merged module between m1 and m2
          // = w1 + w2 -w(m1->m2) -w(m2->m1)
          when( col("e2.exitw").isNotNull,
            col("m1.exitw")+col("m2.exitw")-col("e1.exitw")-col("e2.exitw")
          ).otherwise(
            col("m1.exitw")+col("m2.exitw")-col("e1.exitw")
          ),
          lit( network.probSum ), lit( network.codelength )
        ) as "dL"
      )
    }
    // importantly, dL is symmetric towards src and dst
    // so if both edges (src,dst) and (dst,src) exists
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
      deltaL.groupBy("src").min("dL")
      // filter away rows with non-negative dL
      .filter( "min(dL) <= 0" )
      // join with deltaL to retrieve dst
      .alias("bm")
      .join( deltaL.alias("dL"),
        col("bm.src")===col("dL.src") && col("dL.dL")===col("bm.min(dL)")
      )
      .select( "dL.src", "dL.dst" )
    }

  /***************************************************************************
   * map each network.vertices to a new module
   * according to connected components of m2Merge
   * | id , module |
   ***************************************************************************/
    def calModuleMap( network: Network, m2Merge: DataFrame ): DataFrame = {
      GraphFrame( network.graph.vertices, m2Merge )
      .connectedComponents.setAlgorithm("graphx").run
      .select( col("id"), col("component") as "module" )
    }

  /***************************************************************************
   * calculatenew nodal-modular partitioning scheme
   * difference from moduleMap:
   *   (1) moduleMap verticesare modules
   *       newPartition nodes are all original nodes
   *   (2) moduleMap used only within this function for further calculations
   *       newPartition saved to graph, which is part of final result
   * | id , module |
   ***************************************************************************/
    def calNewPartition( moduleMap: DataFrame, graphVertices: DataFrame ):
    DataFrame = {
      graphVertices.alias("original")
      .join( moduleMap.alias("new"),
        col("new.id") === col("original.id"), "left_outer" )
      .select( col("original.id"),
        when( col("new.id").isNotNull, col("new.module") )
       .otherwise( col("original.module") )
       as "module"
      )
    }

  /***************************************************************************
   * intermediate edges
   * map the associated modules into new module indices
   * if the new indices are the same, they are intramodular connections
   * and will be subtracted from the w_i's
   * if the new indices are different, they are intermodular connections
   * and will be aggregated into w_ij's
   * | src , dst , iWj |
   ***************************************************************************/
    def calInterEdges(
      network: Network, moduleMap: DataFrame
    ): DataFrame = {
      network.graph.edges.alias("edge")
      // map vertex indices to new ones
      .join( moduleMap, col("src") === col("id") )
      .select( col("module") as "m1", col("dst"), col("edge.exitw") )
      .join( moduleMap, col("m1") === col("id") )
      .select( col("m1"), col("module") as "m2", col("edge.exitw") )
      .select( col("m1") as "src", col("m2") as "dst", col("edge.exitw") )
      // aggregate
      .groupBy("src","dst")
      .sum("edge.exitw")
      // rename
      .select( col("src"), col("dst"), col("sum(exitw)") as "exitw" )
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
      .join( moduleMap, "id" )
      // aggregate
      .groupBy("module").sum( "size", "prob", "exitw" )
      // name properly
      .select(
        col("module") as "id",  col("sum(size)") as "size",
        col("sum(prob)") as "prob",  col("sum(exitw)") as "exitw"
      )

      // subtract intramodular edges from modular exit prob
      val intraEdges = interEdges
      // intramodular edges have same indices
      .filter( "src = dst" )
      // aggregate
      .groupBy("src").sum("exitw")
      // name properly
      .select( col("src") as "id", col("sum(exitw)") as "exitw" )

      sumOnly.alias("m")
      .join( intraEdges.alias("e"), col("m.id") === col("e.id"), "left_outer" )
      .select( col("m.id"), col("size"), col("prob"),
        // calculation of exitw
        // subtract intramodular edges from modular exit prob
        // if former exists
        when( col("e.exitw").isNotNull, col("m.exitw") -col("m.exitw") )
        .otherwise( col("m.exitw") ) as "exitw"
      )
      .select( col("id"), col("size"), col("prob"), col("exitw"),
        // calculation of exitq
        // subtract intramodular edges from modular exit prob
        // if former exists
        CommunityDetection.calQ()(
          lit(network.tele), lit(network.nodeNumber),
          col("size"), col("prob"), col("exitw")
        )
        as "exitq"
      )
    }

  /***************************************************************************
   * generate new graph and new network
   ***************************************************************************/

    def calNewNetwork(
      network: Network, newNodeNumber: Long, newCodeLength: Double,
      newModules: DataFrame, newEdges: DataFrame
    ): Network = {
      val newGraph = GraphFrame( newModules, newEdges )
      Network(
        network.tele, newNodeNumber,
        newGraph,
        network.probSum, newCodeLength
      )
    }

    recursiveMerge( 0, network, graph )
  }
}
