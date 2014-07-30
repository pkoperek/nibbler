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
    val filteredValues: Array[Int] = sparkContext.parallelize(1 to 10000).filter(_ < 10).collect()

    "Test query result: " + filteredValues.mkString(",") + "\nParameters used: " + params
  }

  post("/evaluate") {
    val requestAsJson = request.body.parseJson.asJsObject

    val inputFile = requestAsJson.getFields("inputFile")(0).toString()
    val input = sparkContext.textFile(inputFile)

    "Counted: " + input.count()

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

