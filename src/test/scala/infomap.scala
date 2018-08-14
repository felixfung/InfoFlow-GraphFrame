import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class InfoMapTest extends SparkSQLTestSuite
{
  val infoMap = new InfoMap

  ignore("Trivial network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/trivial.net",
      infoMap,
      Set( Row(1,1), Row(2,2) ),
      1.45
    )
    assert( success )
  }

  ignore("Small network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/small.net",
      infoMap,
      Set( Row(1,1), Row(2,1), Row(3,3), Row(4,3) ),
      1.58
    )
    assert( success )
  }

  ignore("Asymmetric network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/small-asym.net",
      infoMap,
      Set( Row(1,1), Row(2,1), Row(3,3) ),
      1.38
    )
    assert( success )
  }

  ignore("Simple network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/simple.net",
      infoMap,
      Set( Row(1,1), Row(2,1), Row(3,1), Row(4,4), Row(5,4), Row(6,4) ),
      2.10 // 2.38
    )
    assert( success )
  }

  ignore("Reproduce Rosvall and Bergstrom 2008 sample network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/rosvall.net",
      infoMap,
      Set(
        Row(1,1), Row(2,1), Row(3,1), Row(4,1), Row(5,1), Row(6,1),
        Row(7,7), Row(8,7), Row(9,7), Row(10,7),
          Row(11,7), Row(12,7), Row(13,7),
        Row(14,14), Row(15,14), Row(16,14), Row(17,14),
          Row(18,14), Row(19,14), Row(20,14), Row(21,14),
        Row(22,22), Row(23,22), Row(24,22), Row(25,22)
      ),
      2.52 // 3.51
    )
    assert( success )
  }

  ignore("Modularity test 1") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/infoflow-vs-modularity1.net",
      infoMap,
      Set(
        Row(1,1), Row(2,1), Row(3,1), Row(4,1),
        Row(5,5), Row(6,5), Row(7,5), Row(8,5),
        Row(9,9), Row(10,9), Row(11,9), Row(12,9),
        Row(13,13), Row(14,13), Row(15,13), Row(16,13)
        //Row(13,9), Row(14,9), Row(15,9), Row(16,9)
      ),
    3.08 // 3.43
    )
    assert( success )
  }

  ignore("Modularity test 2") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/infoflow-vs-modularity2.net",
      infoMap,
      Set(
        Row(1,1), Row(2,1), Row(3,1), Row(4,1),
        Row(5,5), Row(6,5), Row(7,5), Row(8,5),
        Row(9,9), Row(10,9), Row(11,9), Row(12,9),
        Row(13,13), Row(14,13), Row(15,13), Row(16,13)
        //Row(1,1), Row(2,1), Row(3,1), Row(4,1),
        //Row(5,1), Row(6,1), Row(7,1), Row(8,1),
        //Row(9,1), Row(10,1), Row(11,1), Row(12,1),
        //Row(13,1), Row(14,1), Row(15,1), Row(16,1)
      ),
    2.72 // 2.68
    )
    assert( success )
  }

}
