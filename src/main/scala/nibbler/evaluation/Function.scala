package nibbler.evaluation

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