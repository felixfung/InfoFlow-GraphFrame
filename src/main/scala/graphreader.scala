  /***************************************************************************
   * class to read in file
   * and construct GraphFrame
   * vertices: | idx , name , module |
   * edges: | idx1 , idx2 , exit prob. w/o tele |
   *
   * this GraphFrame structure is the one to hold the entire graph information
   * which can be used to generate Network object for community detection
   * usage:
   *   val graph: GraphFrame = GraphFile( sqlContext, filename ).graph
   ***************************************************************************/

import org.apache.spark.sql._
import org.graphframes._

object GraphReader
{
  // simple factory to return the appropriate GraphFile reader
  // based on file extension
  def apply( sqlContext: SQLContext, filename: String ): GraphFrame = {
    val regex = """(.*)\.(\w+)""".r
    filename match {
      case regex(_,ext) => {
        if( ext.toLowerCase == "net" )
          PajekReader( sqlContext, filename )
        else if( ext.toLowerCase == "parquet" )
          ParquetReader( sqlContext, filename )
        else
          throw new Exception(
            "File must be Pajek net file (.net) or Parquet file (.parquet)"
          )
      }
      case _ => throw new Exception("Graph file has no file extension")
    }
  }
}
