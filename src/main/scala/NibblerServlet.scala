import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatra._
import spray.json._


class NibblerServlet(sparkContext: SparkContext) extends ScalatraServlet {

  private val pairGenerator = new PairGenerator
  private val inputParser = new HistdataInputParser

  private def reverse(toReverse: (Double, Long)) = toReverse.swap

  get("/status") {
    val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = getValue(requestAsJson, "inputFile")
    val inputAsText = sparkContext.textFile(inputFile)

    val function = getValue(requestAsJson, "function")
    val functionDeserializeed = Function.buildFunction(function.parseJson.asJsObject)

    val input: RDD[Seq[Double]] = inputAsText.map(inputParser.parseLine)

    val variablePairs = pairGenerator.generatePairs(2)
    val results = new StringBuilder

    for (pair <- variablePairs) {
      val df_dx = functionDeserializeed.differentiate("var_" + pair._1)
      val df_dy = functionDeserializeed.differentiate("var_" + pair._2)

      val df_dx_evaluated = input.map(df_dx.evaluate).zipWithIndex().map(reverse)
      val df_dy_evaluated = input.map(df_dy.evaluate).zipWithIndex().map(reverse)

      val differentiatorType: String = getValueOrDefault(requestAsJson, "numdiff", "backward")
      val differentiator = NumericalDifferentiator(
        differentiatorType,
        pair._1,
        pair._2)

      val numericallyDifferentiated = differentiator.partialDerivative(input).zipWithIndex().map(reverse)

      val result: String = "Results: " +
        " dfdx cnt: " + df_dx_evaluated.count() +
        " dfdy cnt: " + df_dy_evaluated.count() +
        " numdiff cnt: " + numericallyDifferentiated.count()

      println("Partial result: " + result)

      results.append(result)
    }

    results.toString()
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

