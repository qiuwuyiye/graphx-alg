package NMF

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.util.concurrent.TimeUnit

case class Conf(
  edge_pt: String = "hdfs://bda00:8020/user/liping/edges.txt",
  output_pt: String = "hdfs://bda00:8020/user/liping/NMFResult",
  n_partition: Int = 2,
  K: Int = 20,
  learn_rate: Double = 0.01,
  reg: Double = 0.1,
  maxIter: Int = 40
  )

object runNMF {
  val conf = new SparkConf().setAppName("NMF")
  val sc = new SparkContext(conf)

  /**
   * Create a graph by loading edges from a file
   *
   * @param edge_pt   each line format: (vertexId vertexId edgeAttr)
   * @param n_partition  minimum partition size of edges
   */
  def loadEdgeFile(edge_pt: String,
    n_partition: Int = 2): Graph[Int, Double] = {
    val edges: RDD[Edge[Double]] = sc.textFile(edge_pt, n_partition).map { line =>
      val Array(v1, v2, e) = line.trim().split("\\s+", 3)
      Edge(v1.toLong, v2.toLong, e.toDouble)
    }

    Graph.fromEdges(edges, 0, StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
  }


  def main(args: Array[String]) {
    // para decomposition
    val parser = new scopt.OptionParser[Conf]("runNMF") {
      head("NMF", "1.0")
      opt[String]("edge_pt") optional() action { (x, c) =>
        c.copy(edge_pt = x)
      } text ("edge_pt is the input file that includes the graph information")
      opt[String]("output_pt") optional() action { (x, c) =>
        c.copy(output_pt = x)
      } text ("output_pt is the output_pt file that stores two matrix W and H")
      opt[Int]("K") optional() action { (x, c) =>
        c.copy(K = x)
      } validate { x => if (x > 0) success else failure("Option --K must >0")
      } text ("reduceDim is the factorized dimension which is known as K")
      opt[Int]("maxIter") optional() action { (x, c) =>
        c.copy(maxIter = x)
      } validate { x => if (x > 0) success else failure("Option --maxIter must >0")
      } text ("maxIteraton is the max Iteration count of this factorization program")
      opt[Int]("n_partition") optional() action { (x, c) =>
        c.copy(n_partition = x)
      } validate { x => if (x > 0) success else failure("Option --n_partition must >0")
      } text ("n_partition is the min number of RDD's split parts, default is 2")
      opt[Double]("reg") optional() action { (x, c) =>
        c.copy(reg = x)
      } validate { x => if (x > 0.0) success else failure("Option --reg must >0.0")
      } text ("reg is the regularization part, default if 0.1")
      opt[Double]("learn_rate") optional() action { (x, c) =>
        c.copy(learn_rate = x)
      } validate { x => if (x > 0.00) success else failure("Option --learn_rate must >0.00")
      } text ("learning rate, default is 0.01")
    }

    val para: Conf = parser.parse(args, Conf()).get
    val n_partition = para.n_partition
    val K = para.K
    val learn_rate = para.learn_rate
    val reg = para.reg
    val edge_pt = para.edge_pt
    val output_pt = para.output_pt
    val maxIter = para.maxIter
    val graph: Graph[Int, Double] = loadEdgeFile(edge_pt, n_partition)

    val startTime = System.nanoTime()

    //Execute the matrix factorization Method
    val result = NMFImp.run(graph, maxIter, learn_rate, reg, K)
    val estimatedTime = System.nanoTime() - startTime;
    println("algorithm running time:\t" + TimeUnit.NANOSECONDS.toSeconds(estimatedTime))


    // save the result matrix to hdfs
    result.vertices.filter(_._1 < 0).sortBy(_._1, false).saveAsTextFile(output_pt + "H")
    result.vertices.filter(_._1 > 0).sortBy(_._1, true).saveAsTextFile(output_pt + "W")

    //save the new edges to hdfs
    /*result.vertices.filter(_._1 > 0).cartesian(result.vertices.filter(_._1 < 0)).map(a => {
      val product = a._1._2.dot(a._2._2)
      a._1._1.toString + " " + a._2._1.toString + " " + product.toString
    }).saveAsTextFile(output_pt + "product")
    */
    sc.stop()
  }
}