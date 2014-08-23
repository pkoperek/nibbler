import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatra._
import spray.json._


class NibblerServlet(sparkContext: SparkContext) extends ScalatraServlet {

  val timestampParser = new TimestampParser

  get("/status") {
    val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = requestAsJson.getFields("inputFile")(0).toString().dropRight(1).drop(1)
    val input = sparkContext.textFile(inputFile)
    
    val function = requestAsJson.getFields("function")(0)
    val functionDeserializeed = Function.buildFunction(function.asJsObject)
    val input: RDD[Seq[Double]] = inputAsText.map(
      (row: String) => {
        val splitted = row.split(",")
        val timestamp: Long = timestampParser.parse(splitted(0))
        List(timestamp.toDouble) ++ splitted.splitAt(1)._2.map(_.toDouble).toList
      })

    val symbolicallyDifferentiated = input.map(functionDeserializeed.evaluate(_))

    val differentiator = NumericalDifferentiator(
      toString(requestAsJson.getFields("numdiff")),
      0,
      1)
    val numericallyDifferentiated = differentiator.partialDerivative(input)

    

  }

  def toString(fields: Seq[JsValue]): String = {
    fields(0).toString().dropRight(1).drop(1)
  }
}

