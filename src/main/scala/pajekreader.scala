  /***************************************************************************
   * Pajek net file reader
   * file is assumed to be local and read in serially
   ***************************************************************************/

import scala.io.Source
import java.io.FileNotFoundException

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._

object PajekReader
{
  def apply( sqlc: SQLContext, filename: String ): GraphFrame = {
    try {
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
      // to give error if the first line in file is not a section declare
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

        val newSection = line match {
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
                section = "section_def"
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
            case edgeRegex1( src, dst ) =>
              // check that index is in valid range
              if( 1<=src.toLong && src.toLong<=nodeNumber
               && 1<=dst.toLong && dst.toLong<=nodeNumber )
                ( src.toLong, dst.toLong, 1.0 )
              else throw new Exception(
                "Vertex index must be within [1,"+nodeNumber.toString
                +"]: line " +lineNumber.toString
              )
            case edgeRegex2( src, dst, weight ) =>
              // check that index is in valid range
              if( 1<=src.toLong && src.toLong<=nodeNumber
               && 1<=dst.toLong && dst.toLong<=nodeNumber ) {
                // check that weight is not negative
                if( weight.toDouble < 0 ) throw new Exception(
                  "Edge weight must be non-negative: line "+lineNumber.toString
                )
                ( src.toLong, dst.toLong, weight.toDouble )
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

        else if( section != "section_def" )
        {
          throw new Exception("Line does not belong to any sections:"
            +" line "+lineNumber.toString )
        }

  /***************************************************************************
   * prepare for next loop
   ***************************************************************************/
        section = newSection
        lineNumber += 1
      }

  /***************************************************************************
   * check there is at least one vertices section
   ***************************************************************************/
      if( nodeNumber == -1 )
        throw new Exception( "There must be one and only one vertices section" )

  /***************************************************************************
   * import spark.implicits._ to use toDF()
   ***************************************************************************/

      val spark: SparkSession =
      SparkSession
        .builder()
        .appName("InfoFlow")
        .config("spark.master", "local[*]")
        .getOrCreate()
      import spark.implicits._

  /***************************************************************************
   * generate vertices DataFrame
   ***************************************************************************/

      // obtain a DataFrame of vertices
      // which is perhaps missing "unspecified" vertices
      // see later blocks
      val verticesDF_missing = vertices.toDF("id","name","module")
      .cache // since this DF will be used in later blocks, cache it

      // check that vertex indices are unique
      {
        val nonUniqueVertices = 
        verticesDF_missing.select('id,'name,'module,lit(1) as "count")
        .groupBy('id)
        .sum("count")
        .filter("sum(count)>1")

        nonUniqueVertices.rdd.collect.foreach {
          case Row(id,_,_) => throw new Exception
            ( "Vertex index is not unique: "+id.toString )
        }
      }

      // Pajek file format allows unspecified nodes
      // e.g. when the node number is 6 and only node 1,2,3 are specified,
      // nodes 4,5,6 are still assumed to exist with node name = node index
      val verticesDF = List.range(1,nodeNumber+1).toDF("id")
      .select('id, 'id as "default_name")
      .alias("default")
      .join( verticesDF_missing.alias("specific"),
        col("specific.id") === col("default.id"), "left_outer" )
      .select(
        col("default.id"),
        when('name.isNotNull,'name).otherwise('default_name) as "name",
        col("default.id") as "module"
      )

  /***************************************************************************
   * generate edges DataFrame
   ***************************************************************************/

      // aggregate the weights for all same edges
      val edgesDF = edges.toDF("src","dst","exitw")
      .groupBy('src,'dst)
      .sum("exitw")
      .select( col("src"), col("dst"), col("sum(exitw)") as "exitw" )

  /***************************************************************************
   * return GraphFrame
   ***************************************************************************/

      GraphFrame( verticesDF, edgesDF )
    }
    catch {
        case e: FileNotFoundException =>
          throw new Exception("Cannot open file "+filename)
    }
  }
}
