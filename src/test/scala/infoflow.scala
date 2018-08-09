import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class InfoFlowTest extends SparkSQLTestSuite
{
  test("InfoFlow on trivial network") {
    val (success,net1) = CommunityDetectionTest( sqlContext,
      "Nets/trivial.net", new InfoFlow,
      Set( Row(1,1), Row(2,2) ),
      1.45
    )
    assert( success )
  }
}
