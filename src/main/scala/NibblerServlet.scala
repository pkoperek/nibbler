import org.apache.spark.SparkContext

import org.scalatra._
import spray.json._
import math._

object Functions {
  def plus(inputValues: Seq[Double]): Double = {
    var result = 0.0
    for (value <- inputValues) {
      result += value
    }
    result
  }

  def mul(inputValues: Seq[Double]): Double = {
    var result = inputValues(0)
    for (idx <- 1 to inputValues.size) {
      result *= inputValues(idx)
    }
    result
  }

  def div(inputValues: Seq[Double]): Double = {
    var result = inputValues(0)
    for (idx <- 1 to inputValues.size) {
      result /= inputValues(idx)
    }
    result
  }

  def minus(inputValues: Seq[Double]): Double = {
    var result = inputValues(0)
    for (idx <- 1 to inputValues.size) {
      result -= inputValues(idx)
    }
    result
  }
}

class FunctionNode(val functionName: String, val children: Seq[FunctionNode]) {

  def evaluate(inputRow: Seq[Double]): Double = {
    val childrenEvaluated = List[Double]()
    for (child <- children) {
      childrenEvaluated :+ child.evaluate(inputRow)
    }

    val function = resolveFunction(functionName)
    function(childrenEvaluated)
  }

  private def resolveFunction(name: String): (Seq[Double] => Double) = {
    def wrap(toWrap: (Double => Double)): (Seq[Double] => Double) = {
      input: Seq[Double] => toWrap(input(0))
    }

    name match {
      case "plus" => Functions.plus
      case "minus" => Functions.minus
      case "mul" => Functions.mul
      case "div" => Functions.div
      case "sin" => wrap(math.sin)
      case "cos" => wrap(math.cos)
      case "tg" => wrap(math.tan)
      case "exp" => wrap(math.exp)
      case constant => ignoredInput => constant.toDouble
    }
  }

}

object FunctionNode {

  def buildTree(inputAsJson: JsObject): FunctionNode = {
    val children = extractChildren(inputAsJson)
    val childrenAsNodes = List[FunctionNode]()

    for (child <- children) {
      childrenAsNodes :+ buildTree(child)
    }

    val functionName = extractFunctionName(inputAsJson)

    new FunctionNode(functionName, childrenAsNodes)
  }

  private def extractFunctionName(inputAsJson: JsObject): String = {
    inputAsJson.getFields("name")(0).toString()
  }

  private def extractChildren(inputAsJson: JsObject): Seq[JsObject] = {
    inputAsJson.getFields("operands").map(operandAsJsValue => operandAsJsValue.asJsObject)
  }

}

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

