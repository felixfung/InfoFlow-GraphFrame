sealed interface GraphFile(
  sc: SparkContext, sqlContext: SQLContext, filename: String
)
{
  val network: Network
  val name: DataFrame // ( idx: Long, name: String )
}

sealed object GraphFile(
  sc: SparkContext, sqlContext: SQLContext, filename: String
)
{
  def openFile( filename: String ): GraphFile = {
    val extension = filename.split(".").lastOption
    extension match (
      None => throw new Exception("Graph file has no file extension")
      Some(ext) => if( ext.toLowerCase == "net" )
        new PajekFile( sc, sqlContext, filename )
      else if( ext.toLowerCase == "pqt" )
        new ParquetFiles( sc, sqlContext, filename )
    )
  }
}
