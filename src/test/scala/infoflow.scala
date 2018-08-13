import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class InfoFlowTest extends SparkSQLTestSuite
{

  test("InfoFlow trivial network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/trivial.net",
      new InfoFlow,
      Set( Row(1,1), Row(2,2) ),
      1.45
    )
    assert( success )
  }

  ignore("InfoFlow small network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/small.net",
      new InfoFlow,
      Set( Row(1,1), Row(2,1), Row(3,3), Row(4,3) ),
      1.58
    )
    assert( success )
  }

  ignore("InfoFlow asymmetric network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/smallsmall-asymnet.net",
      new InfoFlow,
      Set( Row(1,1), Row(2,1), Row(3,3) ),
      1.38
    )
    assert( success )
  }

  ignore("InfoFlow simple network") {
    val (success,net1) = CommunityDetectionTest(
      sqlContext,
      "Nets/simple.net",
      new InfoFlow,
      Set( Row(1,1), Row(2,1), Row(3,1), Row(4,4), Row(5,4), Row(6,4) ),
      2.38
    )
    assert( success )
  }

}
