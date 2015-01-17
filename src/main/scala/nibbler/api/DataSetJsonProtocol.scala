package nibbler.api

import nibbler.evaluation.DataSet
import spray.json._

object DataSetJsonProtocol extends DefaultJsonProtocol {

  implicit object DataSetJsonFormat extends RootJsonFormat[DataSet] {
    def write(dataSet: DataSet) = {
      JsObject(
        "numberOfRows" -> JsNumber(dataSet.getNumberOfRows),
        "numberOfColumns" -> JsNumber(dataSet.getNumberOfColumns)
      )
    }

    override def read(json: JsValue): DataSet = {
      throw new UnsupportedOperationException
    }
  }

}
