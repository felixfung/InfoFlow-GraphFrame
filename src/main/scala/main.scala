import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.graphframes._

object InfoFlowMain {
  def main( args: Array[String] ): Unit = {

  /***************************************************************************
   * read in config file
   ***************************************************************************/

    // check argument size
    if( args.size > 1 ) {
      println("InfoFlow: requires 0-1 arguments:")
      println("./InfoFlow [alternative config file]")
      return
    }

    // use default or alternative config file name
    val configFileName =
      if( args.size == 0 ) "config.json"
      else /*args.size==1*/ args(0)
    val config = new Config(configFileName)

    // initialize parameters from config file
    val graphFile = config.graphFile
    val merge: MergeAlgo = MergeAlgo.choose( config.mergeAlgo )
    val logFile = new LogFile(
      config.logDir,
      config.logWriteLog, config.rddText,
      config.rddJSon, config.logSteps,
      false
    )

  /***************************************************************************
   * Initialize Spark Context and SQL Context
   ***************************************************************************/
    val conf = new SparkConf()
      .setAppName("InfoFlow")
      .setMaster( config.master )
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  /***************************************************************************
   * read file and solve
   ***************************************************************************/
    val graph0 = GraphFile.openFile( sqlContext, pajekFile ).graph
    val net0 = Network.init( graph0, config.tele )
    val (net1,graph1) = merge( net0, logFile )

  /***************************************************************************
   * save graph
   ***************************************************************************/
    logFile.save( net1, graph1, false, "" )

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
