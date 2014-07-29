import org.apache.spark.{SparkContext, SparkConf}

import scala.util.control.Exception._
import org.scalatra._
import scalate.ScalateSupport
import spray.json._
import DefaultJsonProtocol._
import math._

class Function {

}

class FunctionNode(val functionName: String, val operands: Seq[FunctionNode]) {

  val evaluationFunction = resolve(functionName)

  def evaluate(): Double = {
    1.0
  }

  def resolve(functionName: String): (Double => Double) = {
    try {
      val constant = functionName.toDouble
      return (x: Double) => constant
    } catch {
      case e: NumberFormatException => println("This is not a number (" + functionName + ")... moving on")
    }

    val fn: (Double => Double) = functionName match {
      case "sin" => sin
      case "cos" => cos
    }

    return fn
  }
}

class NibblerServlet(sparkContext: SparkContext) extends ScalatraServlet {

  get("/status") {
    // http://149.156.10.32:9198/status?masterUrl=mesos%3A%2F%2Fzk%3A%2F%2Fmaster%3A2181%2Fmesos
    val masterUrl = params.get("masterUrl")

    if (masterUrl.isDefined) {
      val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter( _ < 10 ).collect()

      "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
    } else {
      "Master URL not specified!"
    }
  }

  post("/evaluate") {
    val expressionAsJson = request.body.parseJson.asJsObject

    //    buildEvaluationFunction(expressionAsJson)
  }

  private def buildEvaluationFunction(input: JsObject): FunctionNode = {
    val functionName = input.getFields("name")(0).toString()
    val operands = input.getFields("operands")

    if (operands.nonEmpty) {
      val operandsAsFunctionNodes = operands.map((operand: JsValue) => buildEvaluationFunction(operand.asJsObject))
      new FunctionNode(functionName, operandsAsFunctionNodes)
    } else {
      new FunctionNode(functionName, List())
    }
  }
}

