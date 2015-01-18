package nibbler.evaluation

object BasicFunctions extends Serializable {

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
