import org.apache.spark.sql._
import org.graphframes._

object ParquetReader
{
  def apply( ss: SparkSession, filename: String ): GraphFrame = {
    val jsonReader = new JsonReader(filename)

    val verticesFile = jsonReader.getVal("Vertex File").toString
    val edgesFile = jsonReader.getVal("Edge File").toString

    val vertices = ss.read.format("parquet").load(verticesFile)
    val edges = ss.read.format("parquet").load(edgesFile)

    GraphFrame( vertices, edges )
  }
}
