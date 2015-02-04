package nibbler.evaluation

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

trait NumericalDifferentiator extends Serializable {
  def partialDerivative(input: RDD[(Long, Seq[Double])]): RDD[(Long, (Double, Double))] = {
    if (!validateInput(input)) {
      throw new IllegalArgumentException("Dataset doesn't contain at least two values in sequence!")
    }

    partialDerivativeInternal(input)
  }

  def partialDerivativeInternal(input: RDD[(Long, Seq[Double])]): RDD[(Long, (Double, Double))]

  def validateInput(input: RDD[(Long,Seq[Double])]): Boolean = {
    val firstSequence = input.first()._2
    firstSequence.size >= 2
  }
}

object NumericalDifferentiator extends Serializable {
  def apply(name: String, differentialQuotientDividend: Int, differentialQuotientDivisor: Int): NumericalDifferentiator = {

    if (differentialQuotientDividend == differentialQuotientDivisor) {
      throw new IllegalArgumentException("dividend index can't be equal to divisor index!")
    }

    name match {
      case "backward" => new BackwardNumericalDifferentiator(differentialQuotientDividend, differentialQuotientDivisor)
      case "central" => new CentralNumericalDifferentiator(differentialQuotientDividend, differentialQuotientDivisor)
    }
  }


  /**
   * x_n - x_n-1
   */
  private class BackwardNumericalDifferentiator(differentialQuotientDividend: Int, differentialQuotientDivisor: Int) extends NumericalDifferentiator {
    override def partialDerivativeInternal(input: RDD[(Long, Seq[Double])]): RDD[(Long, (Double, Double))] = {
      val minuend: RDD[(Long, Seq[Double])] = input
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map(index_plus_1)

      minuend.join(subtrahend).map(differentiate)
    }

    private def differentiate(row: (Long, (Seq[Double], Seq[Double]))) = {
      val minuendRow = row._2._1
      val subtrahendRow = row._2._2

      val differentiated = (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))
      
      (row._1, (minuendRow(differentialQuotientDividend), differentiated)) 
    }

    private def index_plus_1(row: (Long, Seq[Double])) = {
      (row._1 + 1, row._2)
    }
  }

  /**
   * x_n - x_n-1
   */
  private class ForwardNumericalDifferentiator(differentialQuotientDividend: Int, differentialQuotientDivisor: Int) extends NumericalDifferentiator {
    override def partialDerivativeInternal(input: RDD[(Long, Seq[Double])]): RDD[(Long, (Double, Double))] = {
      val minuend: RDD[(Long, Seq[Double])] = input
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map(index_minus_1)

      minuend.join(subtrahend).map(differentiate)
    }

    private def differentiate(row: (Long, (Seq[Double], Seq[Double]))) = {
      val minuendRow = row._2._1
      val subtrahendRow = row._2._2

      val differentiated = (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))
      
      (row._1, (minuendRow(differentialQuotientDividend), differentiated)) 
    }

    private def index_minus_1(row: (Long, Seq[Double])) = {
      (row._1 - 1, row._2)
    }
  }

  /**
   * x_n - x_n-2
   */
  private class CentralNumericalDifferentiator(differentialQuotientDividend: Int, differentialQuotientDivisor: Int) extends NumericalDifferentiator {
    override def partialDerivativeInternal(input: RDD[(Long, Seq[Double])]): RDD[(Long, (Double, Double))] = {
      val minuend: RDD[(Long, Seq[Double])] = input 
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map(index_plus_2)

      minuend.join(subtrahend).map(differentiate)
    }

    private def differentiate(row: (Long, (Seq[Double], Seq[Double]))) = {
      val minuendRow = row._2._1
      val subtrahendRow = row._2._2

      val differentiated = (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))

      (row._1, (minuendRow(differentialQuotientDividend), differentiated))
    }

    private def index_plus_2(row: (Long, Seq[Double])) = {
      (row._1 + 2, row._2)
    }
  }

}

