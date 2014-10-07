package nibbler

import java.io.{FileNotFoundException, File}

import org.apache.spark.rdd.RDD
import org.scalatra._
import spray.json._
import nibbler.DataSetJsonProtocol._

class NibblerServlet(sparkContextService: SparkContextService) extends ScalatraServlet {

  get("/status") {
    val filteredValues: Array[Int] = sparkContextService.getSparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/register") {
    val requestAsJson = request.body.parseJson.asJsObject
    val inputFilePath = getInputFile(requestAsJson)
    val differentiatorType = getNumericalDifferentiatorType(requestAsJson)
    val dataSet = sparkContextService.getDataSetOrRegister(inputFilePath, differentiatorType)
    dataSet.toJson
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = getInputFile(requestAsJson)
    val differentiatorType = getNumericalDifferentiatorType(requestAsJson)
    val functionDeserializeed = getFunction(requestAsJson)
    val inputDataSet = sparkContextService.getDataSetOrRegister(inputFile, differentiatorType)

    new FunctionErrorEvaluator().evaluate(inputDataSet, functionDeserializeed)
  }

  private def getInputFile(requestAsJson: JsObject): String = {
    getValue(requestAsJson, "inputFile")
  }

  private def getNumericalDifferentiatorType(requestAsJson: JsObject): String = {
    getValueOrDefault(requestAsJson, "numdiff", "backward")
  }

  private def getFunction(requestAsJson: JsObject): Function = {
    val function = getValue(requestAsJson, "function")
    val functionDeserializeed = parseFunction(function)
    functionDeserializeed
  }

  private def parseFunction(function: String): Function = {
    FunctionBuilder.buildFunction(function.parseJson.asJsObject)
  }

  def getValueOrDefault(jsonObject: JsObject, key: String, default: String): String = {
    val fields = jsonObject.getFields(key)

    if (fields.size == 0) {
      default
    } else {
      trimQuotes(fields(0))
    }
  }

  def getValue(jsonObject: JsObject, key: String): String = {
    val fields = jsonObject.getFields(key)

    if (fields.size == 0) {
      throw new IllegalArgumentException("Parameter not specified: " + key)
    }

    trimQuotes(fields(0))
  }

  def trimQuotes(toTrim: JsValue): String = {
    trimQuotes(toTrim.toString())
  }

  def trimQuotes(toTrim: String): String = {
    if (toTrim.charAt(0).equals('"') && toTrim.last.equals('"')) {
      toTrim.trim().dropRight(1).drop(1)
    } else {
      toTrim
    }
  }

}