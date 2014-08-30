package nibbler

import spray.json.{JsArray, JsObject}

import scala.collection.mutable.ListBuffer

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

    node(functionName, childrenAsNodes.toList)
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

  def mul(operands: FunctionNode*) = {
    node("mul", operands.toList)
  }

  def minus(operands: FunctionNode*) = {
    node("minus", operands.toList)
  }

  def plus(operands: FunctionNode*) = {
    node("plus", operands.toList)
  }

  def div(operandLeft: FunctionNode, operandRight: FunctionNode) = {
    node("div", List(operandLeft, operandRight))
  }

  def sin(operands: FunctionNode*) = {
    node("sin", operands.toList)
  }

  def cos(operands: FunctionNode*) = {
    node("cos", operands.toList)
  }

  def constant(value: String) = {
    node(value)
  }

  def node(functionName: String): FunctionNode = {
    node(functionName, List())
  }

  def node(functionName: String, children: Seq[FunctionNode]): FunctionNode = {
    new FunctionNode(functionName, children)
  }
}

class Function(functionTree: FunctionNode) extends Serializable {

  def evaluate(input: Seq[Double]): Double = {
    functionTree.evaluate(input)
  }

  def differentiate(differentiateBy: String): Function = {
    new Function(SymbolicDifferentiation.differentiate(functionTree, differentiateBy))
  }

  override def toString: String = {
    "Function[" + functionTree.toString + "]"
  }

}