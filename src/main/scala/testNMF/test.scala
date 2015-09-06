package testNMF

import breeze.numerics.exp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.Vector
import org.apache.spark._
import org.apache.spark.rdd.RDD

object test{

  /**
   *
   * @param train_pt train_path, usually it is a file in a format like(srcId, dstId, attri)
   * @param vali_pt  vali_path, it is also an edge file
   * @param test_pt  test_path, it is an edge file from test datasets.
   * @param output_pt
   */
  case class Conf(
                   train_pt: String = "hdfs://bda00:8020/user/liping/NMFResultproduct",
                   vali_pt: String = "hdfs://bda00:8020/user/liping/vali_edges",
                   test_pt: String = "hdfs://bda00:8020/user/liping/test_edges",
                   output_pt: String = "hdfs://bda00:8020/user/liping/result"
                   )

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("testNMF")
    val sc = new SparkContext(conf)
    val parser = new scopt.OptionParser[Conf]("runNMF") {
      head("NMF", "1.0")
      opt[String]("train_pt") optional() action { (x, c) =>
        c.copy(train_pt = x)
      } text ("train_pt is the input file that includes the graph information")
      opt[String]("vali_pt") optional() action { (x, c) =>
        c.copy(vali_pt = x)
      } text ("vali_pt is the input file that includes the graph information")
      opt[String]("test_pt") optional() action { (x, c) =>
        c.copy(test_pt = x)
      } text ("test_pt is the input file that includes the graph information")
      opt[String]("output_pt") optional() action { (x, c) =>
        c.copy(output_pt = x)
      } text ("output_pt is the output_pt file that stores two matrix W and H")
    }
    val para: Conf = parser.parse(args, Conf()).get
    val train_edges = para.train_pt
    val test_edges = para.test_pt
    val vali_edges = para.vali_pt
    val output = para.output_pt

    //user
    val W: RDD[(Long, Vector)] = sc.textFile(train_edges + "W", 50).map { line =>
      val temp: String = line.trim()
      val len = temp.length
      val tmp = temp.slice(1, len - 1).split(",").map(a => a.toDouble)
      (tmp(0).toLong, Vector(tmp.slice(1, tmp.length)))
    }.cache()

    //item
    val H: RDD[(Long, Vector)] = sc.textFile(train_edges + "H", 50).map { line =>
      val temp: String = line.trim()
      val len = temp.length
      val tmp = temp.slice(1, len - 1).split(",").map(a => a.toDouble)
      (tmp(0).toLong, Vector(tmp.slice(1, tmp.length)))
    }.cache()


    val test = sc.textFile(test_edges, 50).map { line =>
      val Array(v1, v2, e) = line.trim().split("\\s+", 3)
      (v1.toString + v2.toString, e.toDouble)
    }

    val vali: RDD[(String, Double)] = sc.textFile(vali_edges, 50).map { line =>
      val Array(v1, v2, e) = line.trim().split("\\s+", 3)
      (v1.toString + v2.toString, e.toDouble)
    }

    val pairs: RDD[(String, (Vector, Vector))] = W.cartesian(H).map(temp =>(temp._1._1.toString + temp._2._1.toString, (temp._1._2, temp._2._2)))

    def abs(x:Double) = if (x < 0) -x else x

    def square(x:Double) = x * x
    /**
     * Used for test whether it should quit the iteration.
     * @param guess
     * @param x
     * @return
     */
    def isGoogEncough(guess: Double, x:Double) = abs(guess * guess - x) < 0.0001


    /**
     * Guess iteration to calculate square root.
     * @param guess
     * @param x
     * @return
     */
    def sqrtIter(guess: Double, x: Double): Double =
    {
      if (isGoogEncough(guess, x))
        guess
      else
        sqrtIter((guess + x / guess) / 2, x)
    }

    def sqrt(x:Double): Double =
      sqrtIter(1, x)

    println("test error:")
    val count = 2000010

    println("rmse:")
    val sum1 = pairs.join(test).map(a => (a._2._1._1.dot(a._2._1._2) - a._2._2))
    println(sqrt(sum1.sum() / count) * 5)
    println(sum1.max())
    println(sum1.min())
    println("this is rmse, max, min of the result!")
    sc.stop()
  }
}