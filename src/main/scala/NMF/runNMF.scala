package NMF

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.io.PrintWriter
import java.util.concurrent.TimeUnit
import java.io.File
import java.io.IOException

object runNMF {
  val conf = new SparkConf().setAppName("GraphNMFApp").setMaster("local")
  val sc = new SparkContext(conf)

  // TODO: each function should have a comment
  def deleteDir(dir: File) {
    if (dir.isDirectory()) {
      val children: Array[String] = dir.list()
      for (child <- children) {
        deleteDir(new File(dir, child))
      }
    }
    dir.delete()
  }

  // TODO: each function should have a comment
  def LoaderEdgeFile(edgesFile: String,
    edgminPartitions: Int = 2): Graph[Int, Double] = {
    val edgesfile = sc.textFile(edgesFile, edgminPartitions)
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

  def main(args: Array[String]) {
    //val edgesFile: String = args(0)
    val edgminPartitions: Int = 1
    val reducedDim: Int = 20
    //    val maxIteration: Int = 1

    val theta: Double = 0.01
    val lambda: Double = 0.1
    //    val output: String = args(1)
    val withZeroItems: Int = 0
    //val Seq(edgesFile, output, maxIter) = args.toSeq
    val dir = "E:\\jianguoyun\\my\\code\\scala\\bda\\graphx-alg\\"
    val Seq(edgesFile, output, maxIter) = Seq(dir + "edges.txt", dir + "a.txt", "10")
    val maxIteration = maxIter.toInt
    //val loader = new GraphLoader(sc)
    val graph: Graph[Int, Double] = LoaderEdgeFile(edgesFile, edgminPartitions)

    val startTime = System.nanoTime()
    val result = withZeroItems match {
      case 0 => NMFImp.run(graph, maxIteration, theta, lambda, reducedDim)
      case 1 => NMFImp.runWithZero(graph, maxIteration, theta, lambda, reducedDim)
    }
    val estimatedTime = System.nanoTime() - startTime;
    println("algorithm running time:\t" + TimeUnit.NANOSECONDS.toSeconds(estimatedTime))

    deleteDir(new File(output))
    result.vertices.sortBy(_._1, true).saveAsTextFile(output)
    //val a1 = result.vertices.sortBy(_._1,true).map(v=>(v._1,v._2._1))
    //val a2 = result.vertices.sortBy(_._1,true).map(v=>(v._1,v._2._2))
    // a1.saveAsTextFile(output)
    //a2.saveAsTextFile(output+"0")

    //    val out = new PrintWriter(output)
    //    out.println(result.vertices.collect().sortWith((VD1, VD2) => VD1._1 < VD2._1).mkString("\n"))
    //    out.println(result.vertices.collect().mkString("\n"))
    //    out.close()
    sc.stop()
  }
}