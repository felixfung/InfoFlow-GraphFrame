import org.apache.spark.sql._
import org.graphframes._

import scala.util.parsing.json._

sealed class ParquetFiles ( sqlContext: SQLContext, filename: String )
extends GraphFile( sqlContext, filename )
{
  val graph: GraphFrame = {
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

    val verticesFile = getVal( parsedJson, "Vertex File" ).toString
    val edgesFile = getVal( parsedJson, "Edge File" ).toString
    val vertices = sqlContext.read.format("parquet").load(verticesFile)
    val edges = sqlContext.read.format("parquet").load(edgesFile)

    GraphFrame( vertices, edges )
  }
}
