  /***************************************************************************
   * class to read in config file
   ***************************************************************************/

// class that holds parameters for log file
// used in ConfigFile class
object ConfigFile {
  sealed case class LogParams(
    val pathLog:       String,   // plain text file path for merge progress data
    val pathParquet:   String,   // parquet file path for graph data
    val pathRDD:       String,   // RDD text file path for graph data
    val pathJson:      String,   // local Json file path for graph data
    val savePartition: Boolean,  // whether to save partitioning data
    val saveName:      Boolean,  // whether to save node naming data
    val debug:         Boolean,  // whether to print debug details
  )
}

import scala.util.parsing.json._

// class that reads in a Json config file
// usage: val configFile = new ConfigFile("config.json")
//        val master = configFile.master
//        and so on to access other properties
sealed case class ConfigFile( filename: String )
{
  val (
    master: String,
    graphFile: String,
    algorithm: String,
    tele: Double,
    logFile: LogParams
  ) = {
    val wholeFile: String = {
      val source = scala.io.Source.fromFile(filename)
      try source.mkString
      finally source.close
    }

    val parsedJson = JSON.parseFull(wholeFile).get

    // function to retrieve value from parsed Json object
    // usage: val jsonObject: Any
    //        val jsonVal: Any = getVal( jsonObject, "key" )
    def getVal( parsedJson: Any, key: String ) = {
      parsedJson.asInstanceOf[Map[String,Any]] (key)
    }

    // intermediate nested Json object
    val logParams = getVal( parsedJson, "log" )

    // grab values
    (
      getVal( parsedJson, "Master" ).toString,
      getVal( parsedJson, "Graph" ).toString,
      getVal( parsedJson, "Algo" ).toString,
      getVal( parsedJson, "tele" ).toString.toDouble,
      ConfigFile.LogParams(
        getVal( logParams, "log path" ).toString,
        getVal( logParams, "Parquet path" ).toString,
        getVal( logParams, "RDD path" ).toString,
        getVal( logParams, "Json path" ).toString,
        getVal( logParams, "save partition" ).toString.toBoolean,
        getVal( logParams, "save name" ).toString.toBoolean,
        getVal( logParams, "debug" ).toString.toBoolean
      )
    )
  }
}
