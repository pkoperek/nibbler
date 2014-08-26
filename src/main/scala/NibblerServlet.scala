import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatra._
import spray.json._


class NibblerServlet(sparkContext: SparkContext) extends ScalatraServlet {

  private val timestampParser = new TimestampParser
  private val pairGenerator = new PairGenerator

  private def reverse(toReverse: (Double, Long)) = toReverse.swap

  get("/status") {
    val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = toString(requestAsJson.getFields("inputFile"))
    val inputAsText = sparkContext.textFile(inputFile)

    val function = requestAsJson.getFields("function")(0)
    val functionDeserializeed = Function.buildFunction(function.asJsObject)

    val input: RDD[Seq[Double]] = inputAsText.map(
      (row: String) => {
        val splitted = row.split(",")
        val timestamp: Long = timestampParser.parse(splitted(0))
        List(timestamp.toDouble) ++ splitted.splitAt(1)._2.map(_.toDouble).toList
      })

    val variablePairs = pairGenerator.generatePairs(2)
    val results = new StringBuilder

    for (pair <- variablePairs) {
      val df_dx = functionDeserializeed.differentiate("var_" + pair._1)
      val df_dy = functionDeserializeed.differentiate("var_" + pair._2)

      val df_dx_evaluated = input.map(df_dx.evaluate(_)).zipWithIndex().map(reverse)
      val df_dy_evaluated = input.map(df_dy.evaluate(_)).zipWithIndex().map(reverse)

      val differentiatorType: String = toString(requestAsJson.getFields("numdiff"))
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

  def toString(fields: Seq[JsValue]): String = {
    fields(0).toString().dropRight(1).drop(1)
  }
}

