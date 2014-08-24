import spray.json.{JsArray, JsObject}

import scala.collection.mutable.ListBuffer

class Function(functionTree: FunctionNode) {

  def evaluate(input: Seq[Double]): Double = {
    functionTree.evaluate(input)
  }

}

object BasicFunctions {
  private def plus(inputValues: Seq[Double]): Double = {
    var result = 0.0
    for (value <- inputValues) {
      result += value
    }
    result
  }

  private def mul(inputValues: Seq[Double]): Double = {
    var result = inputValues(0)
    for (idx <- 1 to inputValues.size - 1) {
      result *= inputValues(idx)
    }
    result
  }

  private def div(inputValues: Seq[Double]): Double = {
    var result = inputValues(0)
    for (idx <- 1 to inputValues.size - 1) {
      result /= inputValues(idx)
    }
    result
  }

  private def minus(inputValues: Seq[Double]): Double = {
    var result = inputValues(0)
    for (idx <- 1 to inputValues.size - 1) {
      result -= inputValues(idx)
    }
    result
  }

  def resolveFunction(name: String): (Seq[Double] => Double) = {
    def wrap(toWrap: (Double => Double)): (Seq[Double] => Double) = {
      input: Seq[Double] => toWrap(input(0))
    }

    name match {
      case "plus" => BasicFunctions.plus
      case "minus" => BasicFunctions.minus
      case "mul" => BasicFunctions.mul
      case "div" => BasicFunctions.div
      case "sin" => wrap(math.sin)
      case "cos" => wrap(math.cos)
      case "tan" => wrap(math.tan)
      case "exp" => wrap(math.exp)
      case constant => ignoredInput => constant.toDouble
    }
  }

}

class FunctionNode(functionName: String, children: Seq[FunctionNode]) {

  val function: Seq[Double] => Double = BasicFunctions.resolveFunction(functionName)

  def name() = {
    this.functionName
  }

  def evaluate(inputRow: Seq[Double]): Double = {
    val childrenEvaluated = ListBuffer[Double]()
    for (child <- children) {
      childrenEvaluated += child.evaluate(inputRow)
    }

    function(childrenEvaluated.toList)
  }
}

object Function {

  def buildFunction(inputAsJson: JsObject): Function = {
    new Function(buildTree(inputAsJson))
  }

  private def buildTree(inputAsJson: JsObject): FunctionNode = {
    val children = extractChildren(inputAsJson)
    val childrenAsNodes = ListBuffer[FunctionNode]()

    for (child <- children) {
      childrenAsNodes += buildTree(child)
    }

    val functionName = extractFunctionName(inputAsJson)

    new FunctionNode(functionName, childrenAsNodes.toList)
  }

  private def stripQuotes(toStrip: String): String = {
    toStrip.trim.drop(1).dropRight(1)
  }

  private def extractFunctionName(inputAsJson: JsObject): String = {
    stripQuotes(inputAsJson.getFields("function")(0).toString())
  }

  private def extractChildren(inputAsJson: JsObject): Seq[JsObject] = {
    val operands = inputAsJson.getFields("operands")

    if (operands.size != 0)
      operands(0).asInstanceOf[JsArray].elements.map(x => x.asJsObject)
    else
      List()
  }

}


