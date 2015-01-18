package nibbler.evaluation

import scala.collection.mutable.ListBuffer

class FunctionNode(functionName: String, childrenFunctions: Seq[FunctionNode]) extends Serializable {

  val function: Seq[Double] => Double = BasicFunctions.resolveFunction(functionName)

  def name(): String = {
    this.functionName
  }

  def children(): Seq[FunctionNode] = {
    childrenFunctions
  }

  override def toString: String = {
    "(" + name() + "," + (for (child <- children()) yield child.toString()) + ")"
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
