package NMF

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.util.concurrent.TimeUnit


object runNMF {
  val conf = new SparkConf().setAppName("NMF")
  val sc = new SparkContext(conf)
  /** This function loads an edge file and then return a graph, the format is like:(vertexId vertexId edgeAttr)
    *
    * @param edgesFile
    * @param edgesMinPartitions
    * @return A Graph
    */
  def LoaderEdgeFile(edgesFile: String,
    edgesMinPartitions: Int = 2): Graph[Int, Double] = {
    val edgesfile = sc.textFile(edgesFile, edgesMinPartitions)
    val edges: RDD[Edge[Double]] = edgesfile.map(
      line => {
        val edgesfields = line.trim().split("\\s+", 3)
        Edge(edgesfields(0).toLong, edgesfields(1).toLong, edgesfields(2).toDouble)

      }
    )
    val defaultVD = 0
    val nullVert: RDD[(VertexId, Int)] = null
    Graph.fromEdges(edges, defaultVD, StorageLevel.MEMORY_AND_DISK,
      StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
  }

  /**Main function
   *
   * @param args Read input edge file. output file and the iter_num
   */
  def main(args: Array[String]) {
    /**
     * para decomposition
     */

      val parser = new scopt.OptionParser[ParaConfig]("runNMF"){
      head("NMF", "1.0")
      opt[String]("edgesFile") optional() action { (x, c) =>
      c.copy(edgesFile = x)
      } text ("edgesFile is the input file that includes the graph information")
      opt[String]("output") optional() action { (x, c) =>
        c.copy(output = x)
      } text ("output is the output file that stores two matrix W and H")
      opt[Int]("reducedDim") optional() action { (x, c) =>
        c.copy(reducedDim = x)
      } validate { x => if (x > 0) success else failure("Option --reducedDim must >0")
      } text ("reduceDim is the factorized dimension which is known as K")
      opt[Int]("maxIteration") optional() action { (x, c) =>
        c.copy(maxIteration = x)
      } validate { x => if (x > 0) success else failure("Option --maxIteration must >0")
      } text ("maxIteraton is the max Iteration count of this factorization program")
      opt[Int]("edgesMinPartition") optional() action { (x, c) =>
        c.copy(edgesMinPartition = x)
      } validate { x => if (x > 0) success else failure("Option --edgesMinPartition must >0")
      } text ("edgesMinPartition is the min number of RDD's split parts, default is 2")
      opt[Double]("reg") optional() action { (x, c) =>
        c.copy(reg = x)
      } validate { x => if (x > 0.0) success else failure("Option --reg must >0.0")
      } text ("reg is the regularization part, default if 0.1")
      opt[Double]("learnRate") optional() action { (x, c) =>
        c.copy(learnRate = x)
      } validate { x => if (x > 0.00) success else failure("Option --learnRate must >0.00")
      } text("learning rate, default is 0.01")
    }
    val para: ParaConfig = parser.parse(args, ParaConfig()).get
    val edgesMinPartition = para.edgesMinPartition
    val reducedDim = para.reducedDim
    val learnRate = para.learnRate
    val reg = para.reg
    val edgesFile = para.edgesFile
    val output = para.output
    val maxIteration = para.maxIteration
    val graph: Graph[Int, Double] = LoaderEdgeFile(edgesFile, edgesMinPartition)

    val startTime = System.nanoTime()
    /**
     * Eecute the matrix factorization Method
     */
    val result = NMFImp.run(graph, maxIteration, learnRate, reg, reducedDim)
    val estimatedTime = System.nanoTime() - startTime;
    println("algorithm running time:\t" + TimeUnit.NANOSECONDS.toSeconds(estimatedTime))

    /**
     * save the result matrix to hdfs
     */
    result.vertices.filter(_._1 < 0).sortBy(_._1, false).saveAsTextFile(output + "H")
    result.vertices.filter(_._1 > 0).sortBy(_._1, true).saveAsTextFile(output + "W")
    sc.stop()
  }
}