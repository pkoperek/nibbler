package nibbler.api

import nibbler.evaluation.{Function, FunctionBuilder, FunctionErrorEvaluator}
import org.apache.spark.rdd.RDD
import org.scalatra._
import org.scalatra.scalate.ScalateSupport
import spray.json._
import DataSetJsonProtocol._

class NibblerServlet(sparkContextService: SparkContextService) extends ScalatraServlet with ScalateSupport {

  get("/") {
    contentType="text/html"

    ssp("/index")
  }

  get("/status") {
    val filteredValues: Array[Int] = sparkContextService.getSparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/register") {
    val requestAsJson = request.body.parseJson.asJsObject
    val inputFilePath = getValue(requestAsJson, "inputFile")

    val dataSet = sparkContextService.registerDataSet(inputFilePath)
    dataSet.toJson
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = getValue(requestAsJson, "inputFile")
    val input = parse(inputFile)

    val function = getValue(requestAsJson, "function")
    val functionDeserializeed = parseFunction(function)

    val differentiatorType = getValueOrDefault(requestAsJson, "numdiff", "backward")

    new FunctionErrorEvaluator(differentiatorType).evaluate(input, functionDeserializeed)
  }

  private def parseFunction(function: String): Function = {
    FunctionBuilder.buildFunction(function.parseJson.asJsObject)
  }

  private def parse(inputFilePath: String): RDD[Seq[Double]] = {
    sparkContextService.getDataSetOrRegister(inputFilePath).getRawData
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