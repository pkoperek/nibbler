package nibbler

private object SymbolicDifferentiation {

  private val AnyVariable = "var_\\d+".r
  private val constant_0 = Function.node("0.0", List())

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
        mul(
          List(cos(children: _*)) ++ differentiateEach(children, differentiateBy): _*
        )

      case "cos" =>
        mul(
          List(constant("-1.0"))
            ++
            List(sin(children: _*))
            ++
            differentiateEach(children, differentiateBy): _*)

      case "mul" =>
        val differentiatedChildren = differentiateEach(children, differentiateBy)
        val productsWithSingleElementsDifferentiated: Seq[FunctionNode] =
          for (childIndex <- 0 to differentiatedChildren.size - 1)
          yield
            mul(
              children.take(childIndex) ++ List(differentiatedChildren(childIndex)) ++ children.drop(childIndex + 1): _*
            )

        plus(productsWithSingleElementsDifferentiated: _*)

      case "exp" =>
        mul(
          List(nodeToDifferentiate) ++ differentiateEach(children, differentiateBy): _*
        )

      case "div" =>
        val differentiatedChildren = differentiateEach(children, differentiateBy)
        div(
          minus(
            mul(children(1), differentiatedChildren(0)),
            mul(children(0), differentiatedChildren(1))
          ),
          mul(children(1), children(1))
        )

      case "plus" =>
        plus(differentiateEach(children, differentiateBy): _*)

      case "minus" =>
        minus(differentiateEach(children, differentiateBy): _*)

      case `differentiateBy` => constant("1.0")
      case AnyVariable() => constant_0
      case AnyConstant(_) => constant_0
    }
  }

  private def differentiateEach(nodesToDifferentiate: Seq[FunctionNode], differentiateBy: String): Seq[FunctionNode] = {
    for (child <- nodesToDifferentiate) yield differentiate(child, differentiateBy)
  }

  private def mul(operands: FunctionNode*) = Function.mul(operands: _*)

  private def minus(operands: FunctionNode*) = Function.minus(operands: _*)

  private def plus(operands: FunctionNode*) = Function.plus(operands: _*)

  private def div(operandLeft: FunctionNode, operandRight: FunctionNode) = Function.div(operandLeft, operandRight)

  private def sin(operands: FunctionNode*) = Function.sin(operands: _*)

  private def cos(operands: FunctionNode*) = Function.cos(operands: _*)

  private def constant(value: String) = Function.constant(value)
}
