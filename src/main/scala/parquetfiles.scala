import org.apache.spark.sql._
import org.graphframes._

sealed class ParquetFiles ( sqlContext: SQLContext, filename: String )
extends GraphFile( sqlc, filename )
{
  val graph: GraphFrame = {
    val headFile = new BufferedReader( new FileReader(fileName) )
    // read JSON/YAML parameters
    val verticesfile = ???
    val edgesfile = ???
    val vertices = sqlContext.read.parquet(verticesfile)
    val edges = sqlContext.read.parquet(edgesfile)

    GraphFrame( vertices, edges )
  }
}
