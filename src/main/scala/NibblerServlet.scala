import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatra._
import spray.json._


class NibblerServlet(sparkContext: SparkContext) extends ScalatraServlet {

  private val inputParser = new HistdataInputParser

  get("/status") {
    val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
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
    Function.buildFunction(function.parseJson.asJsObject)
  }

  private def parse(inputFilePath: String): RDD[Seq[Double]] = {
    val inputAsText = sparkContext.textFile(inputFilePath)
    inputParser.parse(inputAsText)
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

