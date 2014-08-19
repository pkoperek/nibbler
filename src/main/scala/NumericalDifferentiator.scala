import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

trait NumericalDifferentiator {
  def partialDerivative(input: RDD[Seq[Double]]): RDD[Double]
}

object NumericalDifferentiator {
  def apply(name: String, differentialQuotientDividend: Int, differentialQuotientDivisor: Int): NumericalDifferentiator = {

    if(differentialQuotientDividend == differentialQuotientDivisor) {
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
    override def partialDerivative(input: RDD[Seq[Double]]): RDD[Double] = {

      val minuend: RDD[(Long, Seq[Double])] = input.zipWithIndex().map {
        _.swap
      }
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map((row: (Long, Seq[Double])) => {
        (row._1 - 1, row._2)
      })

      val diff: RDD[Double] = minuend.join(subtrahend).map((row: (Long, (Seq[Double], Seq[Double]))) => {
        val minuendRow = row._2._1
        val subtrahendRow = row._2._2

        val quotient = (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))
        quotient
      })

      diff
    }
  }

  /**
   * x_n - x_n-2
   */
  private class CentralNumericalDifferentiator(differentialQuotientDividend: Int, differentialQuotientDivisor: Int) extends NumericalDifferentiator {
    override def partialDerivative(input: RDD[Seq[Double]]): RDD[Double] = {

      val minuend: RDD[(Long, Seq[Double])] = input.zipWithIndex().map {
        _.swap
      }
      val subtrahend: RDD[(Long, Seq[Double])] = minuend.map((row: (Long, Seq[Double])) => {
        (row._1 - 2, row._2)
      })

      val diff: RDD[Double] = minuend.join(subtrahend).map((row: (Long, (Seq[Double], Seq[Double]))) => {
        val minuendRow = row._2._1
        val subtrahendRow = row._2._2

        val quotient = (minuendRow(differentialQuotientDividend) - subtrahendRow(differentialQuotientDividend)) / (minuendRow(differentialQuotientDivisor) - subtrahendRow(differentialQuotientDivisor))
        quotient
      })

      diff
    }
  }

}

