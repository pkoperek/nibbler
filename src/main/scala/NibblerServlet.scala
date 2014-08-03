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

    children.size match {
      case 0 => {
        functionName.toDouble
      }
      case 1 => {
        val function1Op = resolve1OpFunction(functionName)
        function1Op(childrenEvaluated(0))
      }
      case default => {
        val functionMOp = resolveMultiOpFunction(functionName)
        functionMOp(childrenEvaluated)
      }
    }
  }

  private def resolveMultiOpFunction(name: String): (Seq[Double] => Double) = {
    name match {
      case "plus" => Functions.plus
      case "minus" => Functions.minus
      case "mul" => Functions.mul
      case "div" => Functions.div
    }
  }

  private def resolve1OpFunction(name: String): (Double => Double) = {
    name match {
      case "sin" => math.sin
      case "cos" => math.cos
      case "tg" => math.tan
      case "exp" => math.exp
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

