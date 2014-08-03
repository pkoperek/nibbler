import org.apache.spark.SparkContext

import org.scalatra._
import spray.json._



class NibblerServlet(sparkContext: SparkContext) extends ScalatraServlet {

  get("/status") {
    val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = requestAsJson.getFields("inputFile")(0).toString().dropRight(1).drop(1)
    val input = sparkContext.textFile(inputFile)

    "Counted: " + input.count()

  }

}

