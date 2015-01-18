package nibbler

import nibbler.api.DataSetJsonProtocol
import nibbler.evaluation.DataSet
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.mock.MockitoSugar
import spray.json._
import DataSetJsonProtocol._

class DataSetJsonProtocolTest extends FunSuite with MockitoSugar with ShouldMatchers {

  test("serializes data set to json without rdd") {
    // Given
    val rdd = mock[RDD[Seq[Double]]]
    val dataSet = new DataSet(3, 14, rdd)

    // When
    val dataSetAsJson: String = dataSet.toJson.toString()

    // Then
    dataSetAsJson should (include("numberOfRows") and include("3") and include("numberOfColumns") and include("14"))
  }

  test("UnsupportedOperationException thrown when trying to deserialize data set") {
    // Given
    val jsonAsString = """
                         | {
                         |   "numberOfRows": 12,
                         |   "numberOfColumns": 123
                         | }
                       """.stripMargin

    intercept[UnsupportedOperationException] {
      val json = jsonAsString.parseJson
      json.convertTo[DataSet]
    }
  }
}

