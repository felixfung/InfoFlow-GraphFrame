  /***************************************************************************
   * main function: read in config file, solve, save, exit
   ***************************************************************************/

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
    val configFilename =
      if( args.size == 0 ) "config.json"
      else /*args.size==1*/ args(0)
    val config = ConfigFile(configFilename)

  /***************************************************************************
   * initialize parameters from config file
   ***************************************************************************/
    val master = config.master
    val graphFile = config.graphFile
    val communityDetection = CommunityDetection.choose( config.algorithm )
    val tele = config.tele
    val logFile = new LogFile(
      config.logFile.pathLog,
      config.logFile.pathParquet,
      config.logFile.pathRDD,
      config.logFile.pathJson,
      config.logFile.savePartition,
      config.logFile.saveName,
      config.logFile.debug
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

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder()
      //.appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

  /***************************************************************************
   * read, solve, save
   ***************************************************************************/
    val graph0 = GraphFile.openFile( sqlContext, graphFile ).graph
    val net0 = Network.init( graph0, tele )
    val (net1,graph1) = communityDetection( net0, graph0, logFile )
    logFile.save( net1.graph, graph1, false, "" )

  /***************************************************************************
   * Stop Spark Context
   ***************************************************************************/
    sc.stop
  }
}
