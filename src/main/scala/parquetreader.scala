import org.apache.spark.sql._
import org.graphframes._

object ParquetReader
{
  def apply( sqlContext: SQLContext, filename: String ): GraphFrame = {
    val jsonReader = new JsonReader(filename)

    val verticesFile = jsonReader.getVal("Vertex File").toString
    val edgesFile = jsonReader.getVal("Edge File").toString

    val vertices = sqlContext.read.format("parquet").load(verticesFile)
    val edges = sqlContext.read.format("parquet").load(edgesFile)

    GraphFrame( vertices, edges )
  }
}
