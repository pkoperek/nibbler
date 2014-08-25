import spray.json.{JsArray, JsObject}

import scala.collection.mutable.ListBuffer

class Function(functionTree: FunctionNode) {

  def evaluate(input: Seq[Double]): Double = {
    functionTree.evaluate(input)
  }

  def differentiate(differentiateBy: String): Function = {
    new Function(SymbolicDifferentiation.differentiate(functionTree, differentiateBy))
  }

  override def toString(): String = {
    "Function[" + functionTree.toString + "]"
  }

}

private object SymbolicDifferentiation {

  private val AnyVariable = "var_\\d+".r
  private val constant_0: FunctionNode = node("0.0", List())

  private object AnyConstant {
    def unapply(candidate: String): Option[Double] = {
      try {
        Some(candidate.toDouble)
      } catch {
        case _: Exception => None
      }
    }
  }

  def differentiate(nodeToDifferentiate: FunctionNode, differentiateBy: String): FunctionNode = {
    val children: Seq[FunctionNode] = nodeToDifferentiate.children()

    nodeToDifferentiate.name() match {
      case "sin" =>
        node(
          "mul",
          List(node("cos", children))
            ++
            differentiateEach(children, differentiateBy))

      case "cos" =>
        node(
          "mul",
          List(node("-1.0"))
            ++
            List(node("sin", children))
            ++
            differentiateEach(children, differentiateBy))

      case "mul" => {
        val differentiatedChildren = differentiateEach(children, differentiateBy)
        val productsWithSingleElementsDifferentiated: Seq[FunctionNode] =
          for (childIndex <- 0 to differentiatedChildren.size - 1)
          yield
            node(
              "mul",
              children.take(childIndex) ++ List(differentiatedChildren(childIndex)) ++ children.drop(childIndex + 1)
            )

        node(
          "plus",
          productsWithSingleElementsDifferentiated
        )
      }
      case "plus" =>
        node("plus", differentiateEach(children, differentiateBy))

      case "minus" =>
        node("minus", differentiateEach(children, differentiateBy))

      case `differentiateBy` => node("1.0")
      case AnyVariable() => constant_0
      case AnyConstant(_) => constant_0
    }
  }

  private def differentiateEach(nodesToDifferentiate: Seq[FunctionNode], differentiateBy: String): Seq[FunctionNode] = {
    for (child <- nodesToDifferentiate) yield differentiate(child, differentiateBy)
  }

  private def node(functionName: String): FunctionNode = {
    node(functionName, List())
  }

  private def node(functionName: String, children: Seq[FunctionNode]): FunctionNode = {
    new FunctionNode(functionName, children)
  }
}

private object BasicFunctions {

  private val Variable = "var_(\\d+)".r

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

    def seqSelector(indexToSelect: Int): (Seq[Double] => Double) = {
      input: Seq[Double] => input(indexToSelect)
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
      case Variable(variableIndex) => seqSelector(variableIndex.toInt)
      case constant => ignoredInput => constant.toDouble
    }
  }

}

class FunctionNode(functionName: String, childrenFunctions: Seq[FunctionNode]) {

  val function: Seq[Double] => Double = BasicFunctions.resolveFunction(functionName)

  def name(): String = {
    this.functionName
  }

  def children(): Seq[FunctionNode] = {
    childrenFunctions
  }

  override def toString(): String = {
    "Node(" + name() + "," + (for (child <- children()) yield child.toString()) + ")"
  }

  def evaluate(inputRow: Seq[Double]): Double = {
    if (childrenFunctions.size > 0) {
      processNonLeaf(inputRow)
    } else {
      processLeaf(inputRow)
    }
  }

  private def processLeaf(inputRow: Seq[Double]): Double = {
    function(inputRow)
  }

  private def processNonLeaf(inputRow: Seq[Double]): Double = {
    val childrenEvaluated = ListBuffer[Double]()
    for (child <- childrenFunctions) {
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