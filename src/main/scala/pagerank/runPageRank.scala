package pagerank

import org.apache.spark.graphx.{PartitionStrategy, Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liping on 8/27/15.
 */
object runPageRank {

  val conf = new SparkConf().setAppName("IctPageRank")
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


  case class Conf(
                   edges_pt: String = "hdfs://bda00:8020/user/liping/test_edges_for_pagerank",
                   output_pt: String = "hdfs://bda00:8020/user/liping/pagerank_output",
                   reset_prob: Double = 0.15,
                   partitions: Int = 10,
                   maxIter: Int = 100,
                   tol: Double = 0.0001
                   )

  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Conf]("runPageRank") {
      head("IctPageRank", "1.0")
      opt[String]("edges_pt") optional() action { (x, c) =>
        c.copy(edges_pt = x)
      } text ("edges_pt is the input file that includes the graph information")
      opt[Double]("reset_prob") optional() action { (x, c) =>
        c.copy(reset_prob = x)
      } text ("reset+prob is the probabilty the user will stay on the page and doesn't divert to other pages")
      opt[Double]("tol") optional() action { (x, c) =>
        c.copy(tol = x)
      } text ("tol is a minimum double, it's used to test whether the vertex attribute converges")
      opt[Int]("maxIter") optional() action { (x, c) =>
        c.copy(maxIter = x)
      } text ("maxIter is the max Iteration of the pagerank algorithm")
      opt[Int]("partitions") optional() action { (x, c) =>
        c.copy(partitions = x)
      } text ("partitions is the partition count of the edges file")
      opt[String]("output_pt") optional() action { (x, c) =>
        c.copy(output_pt = x)
      } text ("output_pt is the output_pt file that stores two vertex attribute(the pagerank value)")
    }
    val para: Conf = parser.parse(args, Conf()).get
    val edges_pt = para.edges_pt
    val reset_prob = para.reset_prob
    val maxIter = para.maxIter
    val output = para.output_pt
    val partitions = para.partitions
    val tol = para.tol

    val graph = loadEdgeFile(edges_pt, partitions)

    val result: Graph[(Double, Double), Double] = IctPageRank.run(graph, maxIter, reset_prob, tol)

    result.vertices.map(vdata => (vdata._1, vdata._2._1)).saveAsTextFile(output)

    sc.stop()
  }
}
