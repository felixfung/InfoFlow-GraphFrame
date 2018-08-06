import scala.io.Source
import java.io.FileNotFoundException

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

  /***************************************************************************
   * Pajek net file reader
   * file is assumed to be local and read in serially
   ***************************************************************************/

sealed class PajekFile ( sqlc: SQLContext, val filename: String )
extends GraphFile( sqlc, filename )
{
  val graph: GraphFrame = try {
    // graph elements stored as local list
    // to be converted to DataFrame and stored in GrapheFrame
    // after file reading
    var vertices = new ListBuffer[(Long,String,Long)]()
    var edges = new ListBuffer[(Long,Long,Double)]()

    // regexes to match lines in file
    val starRegex = """\*([a-zA-Z]+).*""".r
    val verticesRegex = """(?i)\*Vertices[ \t]+([0-9]+)""".r
    val vertexRegex = """[ \t]*?([0-9]+)[ \t]+\"(.*)\".*""".r
    val edgeRegex1 = """[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]*""".r
    val edgeRegex2 = """[ \t]*?([0-9]+)[ \t]+([0-9]*)[ \t]+([0-9.]+)[ \t]*""".r

    // store sectioning of file
    // defaults as "__begin"
    // which will give error if the first line in file is not a section declare
    var section: String = "__begin"

    // the number of vertices
    // important since Pajek net format allows nodes to be implicitly declared
    // e.g. when the node number is 6 and only node 1,2,3 are specified,
    // nodes 4,5,6 are still assumed to exist with node name = node index
    var nodeNumber: Long = -1

    var lineNumber = 1 // line number in file, used when printing file error
    // read file serially
    for( line <- Source.fromFile(filename).getLines
      if line.charAt(0) != '%' // skip comments
    ) {
  /***************************************************************************
   * first, check if line begins with '*'
   * which indicates a new section
   * if it is a new section
   * check if it is a vertex section
   * which must be declared once and only once (otherwise throw error)
   * and read in nodeNumber
   ***************************************************************************/

      section = line match {
        // line is section declarator, modify section
        case starRegex(id) => {
          line match {
            case starRegex(expr) => {
              val newSection = expr.toLowerCase
              // check that new section is valid
              if( newSection!="vertices"
                && newSection!="arcs" && newSection!="arcslist"
                && newSection!="edges" && newSection!="edgeslist"
              )
                throw new Exception( "Pajek file format only accepts"
                  +" Vertices, Arcs, Edges, Arcslist, Edgeslist"
                  +" as section declarator: line "+lineNumber )
              // check there is no more than one vertices section
              if( newSection == "vertices" ) {
                if( nodeNumber != -1 )
                  throw new Exception( "There must be one and only one"
                    +"vertices section" )
                // read nodeNumber
                nodeNumber = line match {
                  case verticesRegex(expr) => expr.toLong
                  case _ => throw new Exception( "Cannot read node number:"
                    +" line "+lineNumber.toString )
                }
              }
              newSection
            }
          }
        }
        // line is not section declarator,
        // section does not change
        case _ => section
      }

  /***************************************************************************
   * Read vertex information
   ***************************************************************************/
      if( section == "vertices" ) {
        val newVertex = line match {
          case vertexRegex( idx, name ) =>
            if( 1<=idx.toLong && idx.toLong<=nodeNumber )
              ( idx.toLong, name, idx.toLong )
            // check that index is in valid range
            else throw new Exception(
              "Vertex index must be within [1,"+nodeNumber.toString
              +"]: line " +lineNumber.toString
            )
          // check vertex parsing is correct
          case _ => throw new Exception(
            "Vertex definition error: line " +lineNumber.toString
          )
        }
        vertices += newVertex
      }

  /***************************************************************************
   * Read edge information
   ***************************************************************************/
      else if( section=="edges" || section=="arcs" ) {
        val newEdge = line match {
          case edgeRegex1( idx1, idx2 ) =>
            // check that index is in valid range
            if( 1<=idx1.toLong && idx1.toLong<=nodeNumber
             && 1<=idx2.toLong && idx2.toLong<=nodeNumber )
              ( idx1.toLong, idx2.toLong, 1.0 )
            else throw new Exception(
              "Vertex index must be within [1,"+nodeNumber.toString
              +"]: line " +lineNumber.toString
            )
          case edgeRegex2( idx1, idx2, weight ) =>
            // check that index is in valid range
            if( 1<=idx1.toLong && idx1.toLong<=nodeNumber
             && 1<=idx2.toLong && idx2.toLong<=nodeNumber ) {
              // check that weight is not negative
              if( weight.toDouble < 0 ) throw new Exception(
                "Edge weight must be non-negative: line "+lineNumber.toString
              )
              ( idx1.toLong, idx2.toLong, weight.toDouble )
            }
            else throw new Exception(
              "Vertex index must be within [1,"+nodeNumber.toString
              +"]: line " +lineNumber.toString
            )
          // check vertex parsing is correct
          case _ => throw new Exception(
            "Edge definition error: line " +lineNumber.toString
          )
        }
        edges += newEdge
      }

  /***************************************************************************
   * Read edge list information
   ***************************************************************************/
      else if( section=="edgeslist" || section=="arcslist" ) {
        // obtain a list of vertices
        val vertices = line.split("\\s+").filter(x => !x.isEmpty)
        // obtain a list of edges
        val newEdges = vertices.slice( 1, vertices.length )
        .map {
          case toVertex => ( vertices(0).toLong, toVertex.toLong, 1.0 )
        }
        // append new list to existing list of edges
        edges ++= newEdges
      }

      else {
        throw new Exception("Line does not belong to any sections:"
          +" line "+lineNumber.toString )
      }

      lineNumber += 1
    }

  /***************************************************************************
   * check there is at least one vertices section
   ***************************************************************************/
    if( nodeNumber == -1 )
      throw new Exception( "There must be one and only one vertices section" )

  /***************************************************************************
   * generate vertices DataFrame, plus tidy up
   ***************************************************************************/

    val spark: SparkSession =
    SparkSession
      .builder()
      .appName("InfoFlow")
      .config("spark.master", "local[*]")
      .getOrCreate()
    import spark.implicits._

    // obtain a DataFrame of vertices
    // which is perhaps missing "unspecified" vertices
    // see later blocks
    val verticesDF_missing = vertices.toDF("idx","name","module")
    .cache // since this DF will be used in later blocks, cache it

    // check that vertex indices are unique
    {
      val nonUniqueVertices = 
      verticesDF_missing.select('idx,'name,'module,lit(1) as "count")
      .groupBy("idx")
      .sum("count")
      .filter("count>1")

      nonUniqueVertices.rdd.collect.foreach {
        case Row(idx,_,_) => throw new Exception
          ( "Vertex index is not unique: "+idx.toString )
      }
    }

    // Pajek file format allows unspecified nodes
    // e.g. when the node number is 6 and only node 1,2,3 are specified,
    // nodes 4,5,6 are still assumed to exist with node name = node index
    val verticesDF = List.range(1,nodeNumber+1).toDF("idx")
    .select('idx,lit('idx.toString) as "default_name")
    .join( verticesDF_missing, 'idx, "left_outer" )
    .select('idx, when(col("name").isNotNull,'name).otherwise('default_name),'module)

  /***************************************************************************
   * generate edges DataFrame, plus tidy up
   ***************************************************************************/

    // aggregate the weights for all same edges
    val edgesDF = edges.toDF("idx1","idx2","exitw")
    .groupBy('idx1,'idx2)
    .sum("exitw")

  /***************************************************************************
   * return GraphFrame
   ***************************************************************************/

    GraphFrame( verticesDF, edgesDF )
  }
  catch {
      case e: FileNotFoundException =>
        throw new Exception("Cannot open file "+filename)
      case e: Exception =>
        throw e
      case _: Throwable =>
        throw new Exception("Error reading file line")
  }
}
