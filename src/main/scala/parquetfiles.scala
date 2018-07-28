sealed class ParquetFiles extends GraphFile (
  sc: SparkContext, sqlContext: SQLContext, filename: String
)
{
  val network: Network = {
    val headFile = new BufferedReader( new FileReader(fileName) )
    // read JSON/YAML parameters
    val nodesfile = ???
    val edgesfile = ???
    val nodes = sqlContext.read.parquet(nodesfile)
    val edges = sqlContext.read.parquet(edgesfile)

    Network.init( GraphFrame(nodes,edges) )
  }
}
