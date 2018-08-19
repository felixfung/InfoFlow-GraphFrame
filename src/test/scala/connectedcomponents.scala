import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class ConnectedComponentTest extends SparkSQLTestSuite
{
  /***************************************************************************
   * routine to check weakly connected component labeling result
   ***************************************************************************/
  def checkConnectedComponent(
    vertices0: Set[(Int,Int)], edges0: Set[(Int,Int)]
  ): Boolean = {
    val vertices1 = vertices0.map( _._1 )
    val graph0 = GraphFrame( vertices1.toDF, edges0.toDF )
    val graph1 = ConnectedComponent.weak( graph0.toDF )
    graph1.vertices.collect.toSet == vertices0
  }

  /***************************************************************************
   * test cases
   ***************************************************************************/

  test("Trival graph") {
    val vertices = Set( (1,1), (2,2) )
    val edges = Set()
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Trival disjoint graph") {
    val vertices = Set( (1,1), (2,1), (3,3), (4,3) )
    val edges = Set( (1,2), (4,3) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Trival graph with self-connecting edge") {
    val vertices = Set( (1,1) )
    val edges = Set( (1,1) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Trival graph with loop") {
    val vertices = Set( (1,1), (2,1) )
    val edges = Set( (1,2), (2,1) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Loop size-4") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1) )
    val edges = Set( (1,2), (2,3), (3,4), (4,1) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Loop size-4 with duplicated edge") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1) )
    val edges = Set( (1,2), (2,3), (3,4), (4,1), (4,1) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Wrong sided loop") {
    val vertices = Set( (1,1), (2,1), (3,1) )
    val edges = Set( (1,2), (1,3), (2,3) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Figure-8 loop") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1) )
    val edges = Set( (1,2), (2,3), (3,1), (2,4), (4,2) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("O-o loop") {
    val vertices = Set( (1,1), (2,1), (3,1) )
    val edges = Set( (1,2), (2,3), (3,1), (3,2) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("T-shaped graph") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1) )
    val edges = Set( (1,2), (2,3), (2,4) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("6-shaped graph") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1) )
    val edges = Set( (1,2), (2,3), (3,1), (2,4) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Small graph 1") {
    val vertices = Set( (1,1), (2,2), (3,2), (4,1), (5,2), (7,7), (9,7) )
    val edges = Set( (1,4), (2,3), (5,3), (9,7) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Small graph 2") {
    val vertices = Set( (1,1), (2,2), (3,2), (4,1), (5,2), (7,7), (9,7) )
    val edges = Set( (1,4), (3,7), (5,2), (5,4), (9,3) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("All connected graph 1") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1) )
    val edges = Set( (1,7), (2,3), (3,1), (4,5), (6,3), (7,5) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("All connected graph 2") {
    val vertices = Set( (1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1) )
    val edges = Set( (1,2), (2,3), (3,4), (4,5), (5,6), (6,7) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Big graph 1") {
    val vertices = Set(
      ( 1, 1), ( 2, 1), ( 3, 1), ( 4, 1), ( 5, 5), ( 6, 6), ( 7, 7),
          ( 8, 1), ( 9, 1), (10, 1),
      (11,11), (12,12), (13, 1), (14,11), (15,15), (16, 1), (17, 1),
          (18, 1), (19,11), (20,20),
      (21,20), (22, 1), (23,23), (24, 1), (25, 1), (26,11), (27,27),
          (28, 5), (29, 1), (30,11),
      (31, 1), (32, 1), (33, 1), (34,34), (35, 1)
    val edges = Set(
      (4,32), (7,33), (2,10), (10,29), (10,31),
      (4,10), (13,31), (18,34), (20,21), (14,30),
      (10,18), (24,25), (10,33), (10,17), (14,19),
      (16,18), (26,30), (11,30), (1,9), (8,18),
      (10,22), (1,18), (25,35), (25,33), (5,28), (3,25)
    )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Big all connected graph") {
    val vertices = Set(
      (1,1), (2,1), (3,1), (4,1), (7,1), (9,1), (10,1),
      (13,1), (16,1), (17,1), (18,1), (22,1), (24,1),
      (25,1), (29,1), (31,1), (32,1), (33,1), (34,1), (35,1)
    )
    val edges = Set(
        (4,32), (7,33), (2,10), (10,29), (10,31), (4,10), (13,31), (18,34),
        (10,18), (24,25), (10,33), (10,17), (16,18), (1,9), (8,18),
        (10,22), (1,18), (25,35), (25,33), (3,25)
    )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Weird graph") {
    val vertices = Set(
      (1,1), (2,1), (3,1), (5,1) (6,1), ((9,1), (17,1), (35,1), (40,1),
      (59,1), (68,1), (85,1), (269,1), (2660,1)
    )
    val edges = Set(
      (269,2660), (269,59), (2660,59), (2660,6), (9,2660),
      (17,2660), (2660,35), (2660,3), (2660,5), (2,2660),
      (2660,40), (1,2660), (2660,68), (85,2660)
    )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Random graph 1") {
    val vertices = Set(
      (-4,-4), (6,-4), (0,0), (1,0), (2,0), (3,0), (4,4), (6,4)
    )
    val edges = Set( (1,2), (1,1), (-4,6), (4,6), (2,3), (2,0), (2,1) )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Random graph 2") {
    val vertices = Set(
      (-34,-34), (-1,-1), (1,1), (2,1), (3,1), (4,-34), (6,-34), (8,-1)
    )
    val edges = Set(
      (-1,8), (1,1), (-34,6), (4,6), (2,3), (2,0), (2,1)
    )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }

  test("Random graph 3") {
    val vertices = Set(
      (-3,-3), (1,-3), (2,2), (3,2), (4,4), (5,4), (6,2), (7,2), (8,-3), (9,-3),
      (13,13), (14,2), (19,-3) (42,13), (59,13)
    )
    val edges = Set(
      (3,2), (6,3), (-3,9), (1,9), (4,5), (3,7), (8,9),
      (42,59), (59,13), (12,-1), (3,6), (1,19), (14,2)
    )
    assert( checkConnectedComponent( vertices0,edges), vertices1 ) )
  }
}
