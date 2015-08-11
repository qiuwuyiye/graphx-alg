package NMF

import org.apache.spark
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark.util.Vector
import java.io.PrintWriter

/**
 * Graph NMF algorithm implementation.
 *
 * Implementation Idea based on Pregel
 *
 * During the iteration, design the direction of the message propagating
 * The initial iteration number is 1
 * When iteration number is odd, forward propagate
 * 	update matrix W and use matrix H to propagate
 * When iteration number is even, back propagate
 * 	update matrix H and use matrix W to propagate
 *
 * vertex attribute is [(Vector,Vector)], the first Vector is Vector W, and
 * the other is Vector H
 *
 * update Rule:
 * W_i <- (1-\theta*\lambda)*W_i + \theta sigma{ (d_ij - W_i*H_j) * H_j }
 * H_j <- (1-\theta*\lambda)*H_j + \theta sigma{ (d_ij - W_i*H_j) * W_i }
 *
 * `theta` is the step size
 * `lambda` is the normalized item
 *
 */
object NMFImp {
  /**
   * Run GraphNMF on fixed iteration algorithm returning a graph with
   * vertex attributes containing the two Vectors which is Vector W and Vector H
   * and edge attributes containing the edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   *
   * @param graph the graph on which to run NMF, the edge attribute must be Double
   * @param maxIteration the max iteration
   * @param theta the step size
   * @param lambda the normalization item
   * @param reducedDim the reduced Dimension in NMF algorithm
   *
   * @return the graph containing the two Vectors which is Vector W and Vector H
   * and edge attributes containing the edge weight.
   *
   */
  def run[VD: ClassTag](graph: Graph[VD, Double],
                        maxIterations: Int = Int.MaxValue,
                        theta: Double = 0.01,
                        lambda: Double = 0.1,
                        reducedDim: Int = 50) = {

    def forwardVertexProgram(id: VertexId, attri: (Vector, Vector), msgSum: Vector): (Vector, Vector) = {
      val scale = 1 - theta * lambda
      val intercept = theta * msgSum
      val newV = scale * attri._1 + intercept
      if (newV.elements.count(elem => elem < 0.0) == 0)
        (newV, attri._2)
      else {
        val newElementsNonZero = newV.elements.map(elem => if (elem > 0.0) elem else 0.0)
        (Vector(newElementsNonZero), attri._2)
      }
    }
    def backVertexProgram(id: VertexId, attri: (Vector, Vector), msgSum: Vector): (Vector, Vector) = {
      val scale = 1 - theta * lambda
      val intercept = theta * msgSum
      val newV = scale * attri._2 + intercept
      if (newV.elements.count(elem => elem < 0.0) == 0)
        (attri._1, newV)
      else {
        val newElementsNonZero = newV.elements.map(elem => if (elem > 0.0) elem else 0.0)
        (attri._1, Vector(newElementsNonZero))
      }
    }
    def forwardSendMessage(edge: EdgeTriplet[(Vector, Vector), Double]) = {
      Iterator((edge.srcId, (edge.attr - edge.srcAttr._1.dot(edge.dstAttr._2)) * edge.dstAttr._2))
    }
    def backSendMessage(edge: EdgeTriplet[(Vector, Vector), Double]) = {
      Iterator((edge.dstId, (edge.attr - edge.srcAttr._1.dot(edge.dstAttr._2)) * edge.srcAttr._1))
    }
    def messageCombiner(a: Vector, b: Vector): Vector = a + b

    // initiate each Vertex's vector W and vector H whose dimension is reducedDim on
    var curGraph: Graph[(Vector, Vector), Double] = graph
      .mapVertices((vid, vdata) =>
      (Vector(Array.fill(reducedDim)(Random.nextDouble)),
        Vector(Array.fill(reducedDim)(Random.nextDouble)))).cache()

    //    var curGraph = nmfGraph //.cache()
    var messages = curGraph.mapReduceTriplets(forwardSendMessage, messageCombiner)
    var activeMessages = messages.count()
    var curIteration: Int = 1

    var prevGraph: Graph[(Vector, Vector), Double] = null
    while (activeMessages > 0 && (curIteration - 1) / 2 < maxIterations) {
      if ((curIteration - 1) % 2 == 0) {
        //logDebug("Graph Information\n")
        //logDebug(curGraph.vertices.collect().mkString("\n"))
        //logDebug("GraphNMF interation:" + ((curIteration + 1) / 2).toString)
        //logDebug("forward propagating\nprapagating messages:")
        //logDebug(messages.collect().mkString("\n"))

        val newVerts: VertexRDD[(Vector, Vector)] = curGraph.vertices.innerJoin(messages)(forwardVertexProgram).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        curGraph.cache()

        val oldMessages = messages
        messages = curGraph.mapReduceTriplets(backSendMessage, messageCombiner).cache()
        oldMessages.unpersist(blocking = false)
        newVerts.unpersist(blocking = false)
      } else {
        //logDebug("back propagating\nprapagating messages:")
        //logDebug(messages.collect().mkString("\n"))

        val newVerts = curGraph.vertices.innerJoin(messages)(backVertexProgram).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        curGraph.cache()

        val oldMessages = messages
        messages = curGraph.mapReduceTriplets(forwardSendMessage, messageCombiner).cache()
        oldMessages.unpersist(blocking = false)
        newVerts.unpersist(blocking = false)
      }
      activeMessages = messages.count()

      prevGraph.unpersistVertices(blocking = false)
      prevGraph.edges.unpersist(blocking = false)
      curIteration += 1
    }
    curGraph
  }
  /**
   * Run GraphNMF on fixed iteration algorithm returning a graph with
   * vertex attributes containing the two Vectors which is Vector W and Vector H
   * and edge attributes containing the edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   *
   * @param graph the graph on which to run NMF, the edge attribute must be Double
   * @param maxIteration the max iteration
   * @param theta the step size
   * @param lambda the normalization item
   * @param reducedDim the reduced Dimension in NMF algorithm
   *
   * @return the graph containing the two Vectors which is Vector W and Vector H
   * and edge attributes containing the edge weight.
   *
   */
  def runWithZero[VD: ClassTag](graph: Graph[VD, Double],
                                maxIterations: Int = Int.MaxValue,
                                theta: Double = 0.01,
                                lambda: Double = 0.1,
                                reducedDim: Int = 2) = {
    //    val matrixWAccumulator = sc.accumulator(new Array[Double](reducedDim * reducedDim), "MatrixW")
    //    val matrixHAccumulator = sc.accumulator(new Array[Double](reducedDim * reducedDim), "MatrixH")

    var MatrixH = new Array[Double](reducedDim * reducedDim)
    var MatrixW = new Array[Double](reducedDim * reducedDim)

    /**
     * Wi * MatrixH (j) = Wi * Matrix(*,j)
     * 					= Wi * Array(i+reducedDim * j) i=0,...,reducedDim-1
     */
    def mutiplyVM(vec: Vector, arr: Array[Double]): Vector = {
      var result = new Array[Double](reducedDim)
      val vecElems = vec.elements
      for (i <- 0 to reducedDim - 1) {
        for (j <- 0 to reducedDim - 1) {
          result(i) += (vecElems(j) * arr(j + reducedDim * i))
        }
      }
      Vector(result)
    }
    def forwardVertexProgram(id: VertexId, attri: (Vector, Vector), msgSum: Vector): (Vector, Vector) = {
      val scale = 1 - theta * lambda
      val intercept = theta * (msgSum - mutiplyVM(attri._1, MatrixH))
      val newV = scale * attri._1 + intercept
      if (newV.elements.count(elem => elem < 0.0) == 0)
        (newV, attri._2)
      else {
        val newElementsNonZero = newV.elements.map(elem => if (elem > 0.0) elem else 0.0)
        (Vector(newElementsNonZero), attri._2)
      }
    }
    def backVertexProgram(id: VertexId, attri: (Vector, Vector), msgSum: Vector): (Vector, Vector) = {
      val scale = 1 - theta * lambda
      val intercept = theta * (msgSum - mutiplyVM(attri._2, MatrixW))
      val newV = scale * attri._2 + intercept
      if (newV.elements.count(elem => elem < 0.0) == 0)
        (attri._1, newV)
      else {
        val newElementsNonZero = newV.elements.map(elem => if (elem > 0.0) elem else 0.0)
        (attri._1, Vector(newElementsNonZero))
      }
    }
    def forwardSendMessage(edge: EdgeTriplet[(Vector, Vector), Double]) = {
      Iterator((edge.srcId, edge.dstAttr._2.multiply(edge.attr)))
    }
    def backSendMessage(edge: EdgeTriplet[(Vector, Vector), Double]) = {
      Iterator((edge.dstId, edge.srcAttr._1.multiply(edge.attr)))
    }
    def messageCombiner(a: Vector, b: Vector): Vector = a + b

    // initiate each Vertex's vector W and vector H whose dimension is reducedDim on
    var curGraph: Graph[(Vector, Vector), Double] = graph
      .mapVertices((vid, vdata) =>
      (Vector(Array.fill(reducedDim)(Random.nextDouble)),
        Vector(Array.fill(reducedDim)(Random.nextDouble)))).cache()

    //    var curGraph = nmfGraph //.cache()
    var messages = curGraph.mapReduceTriplets(forwardSendMessage, messageCombiner)
    var activeMessages = messages.count()
    var curIteration: Int = 1

    var prevGraph: Graph[(Vector, Vector), Double] = null
    while (activeMessages > 0 && (curIteration - 1) / 2 < maxIterations) {
      if ((curIteration - 1) % 2 == 0) {
        //logDebug("Graph Information\n")
        //logDebug(curGraph.vertices.collect().mkString("\n"))
       // logDebug("GraphNMF interation:" + ((curIteration + 1) / 2).toString)
        //logDebug("forward propagating\nprapagating messages:")
        //logDebug(messages.collect().mkString("\n"))

        MatrixH = new Array[Double](reducedDim * reducedDim)
        curGraph.vertices.sortBy(_._1, true).map(vertexElem => { //sortWith((VD1, VD2) => VD1._1 < VD2._1).
        // for each vertex compute Hj'*Hj and then add them together
        val h = vertexElem._2._2
          var i = 0
          var j = 0
          for (elemi <- h.elements) {
            i = 0
            for (elemj <- h.elements) {
              var k = i + reducedDim * j
              MatrixH(k) = MatrixH(k) + elemi * elemj
              i += 1
            }
            j += 1
          }
        })

        MatrixW = new Array[Double](reducedDim * reducedDim)
        curGraph.vertices.sortBy(_._1, true).map(vertexElem => {
          val w = vertexElem._2._1
          var i = 0
          var j = 0
          for (elemi <- w.elements) {
            i = 0
            for (elemj <- w.elements) {
              var k = i + reducedDim * j
              MatrixW(k) = MatrixW(k) + elemi * elemj
              i += 1
            }
            j += 1
          }
        })

        val newVerts: VertexRDD[(Vector, Vector)] = curGraph.vertices.innerJoin(messages)(forwardVertexProgram).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        curGraph.cache()

        val oldMessages = messages
        messages = curGraph.mapReduceTriplets(backSendMessage, messageCombiner).cache()
        oldMessages.unpersist(blocking = false)
        newVerts.unpersist(blocking = false)
      } else {
        //logDebug("Graph Information\n")
        //logDebug(curGraph.vertices.collect().mkString("\n"))
       // logDebug("back propagating\nprapagating messages:")
        //logDebug(messages.collect().mkString("\n"))

        val newVerts = curGraph.vertices.innerJoin(messages)(backVertexProgram).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        curGraph.cache()

        val oldMessages = messages
        messages = curGraph.mapReduceTriplets(forwardSendMessage, messageCombiner).cache()
        oldMessages.unpersist(blocking = false)
        newVerts.unpersist(blocking = false)
      }
      activeMessages = messages.count()

      prevGraph.unpersistVertices(blocking = false)
      prevGraph.edges.unpersist(blocking = false)
      curIteration += 1
    }
    curGraph
  }
}
