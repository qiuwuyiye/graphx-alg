package NMF

import org.apache.spark
import org.apache.spark.graphx._
//import org.apache.spark.mllib.linalg.Vector
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark.util.Vector


/**
 * Graph NMF algorithm implementation.
 *
 * Implementation Idea based on Pregel
 *
 * During the iteration, design the direction of the message propagating
 * The initial iteration number is 1
 * When iteration number is odd, forward propagate
 * update matrix W and use matrix H to propagate
 * When iteration number is even, back propagate
 * update matrix H and use matrix W to propagate
 *
 * vertex attribute is [(Vector,Vector)], the first Vector is Vector W, and
 * the other is Vector H
 *
 * update Rule:
 * W_i <- (1-learnRate*reg)*W_i + learnRate sigma{ (d_ij - W_i*H_j) * H_j }
 * H_j <- (1-learnRate*reg)*H_j + learnRate sigma{ (d_ij - W_i*H_j) * W_i }
 *
 * `learnRate` is the learning rate
 * `reg` is the regularization coefficient
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
   * @param maxIterations the max iteration
   * @param learnRate the learn rate
   * @param reg the normalization item
   * @param reducedDim the reduced Dimension in NMF algorithm
   *
   * @return the graph containing the two Vectors which is Vector W and Vector H
   *         and edge attributes containing the edge weight.
   *
   */
  def run[VD: ClassTag](graph: Graph[VD, Double],
    maxIterations: Int = 10,
    learnRate: Double = 0.01,
    reg: Double = 0.1,
    reducedDim: Int = 50) = {

    /** Update graph vertex attribute function.
     *
     * @param id  Vertex id
     * @param attri  Vertex attribute
     * @param msgSum The combined message sum from other vertex
     * @return A new vector and it's the vertex's new attribute
     */
    def VertexProgram(id: VertexId, attri: Vector, msgSum: Vector): Vector = {
      val scale = 1 - learnRate * reg
      val intercept = learnRate * msgSum
      val newV: Vector = scale * attri + intercept
      // set the negative value to zero
      Vector(newV.elements.map(math.max(_, 0)))
    }

    /**Send messages to the source vertex with the opposite edge direction
     *
     * @param triplet [vertex attribute, edge attribute, message]
     */
    def forwardSendMessage(triplet: EdgeContext[Vector, Double, Vector]) {
      triplet.sendToSrc((triplet.attr - triplet.srcAttr.dot(triplet.dstAttr)) * triplet.dstAttr)
    }

    /**Send messages to the destination vertex with edge direction
     *
     * @param triplet [vertex attribute, edge attribute, message]
     */
    def backSendMessage(triplet: EdgeContext[Vector, Double, Vector]){
      triplet.sendToDst((triplet.attr - triplet.srcAttr.dot(triplet.dstAttr)) * triplet.srcAttr)
    }

    /** Combine the two messages and return the sum of them.
     *
     * @param a A vector
     * @param b A vector
     * @return  Sum of a and b
     */
    def messageCombiner(a: Vector, b: Vector): Vector = a + b
    /** Initiate each Vertex's vector W and vector H whose dimension is reducedDim
      *
      */
    var curGraph: Graph[Vector, Double] = graph.mapVertices { (vid, vdata) =>
      Vector(Array.fill(reducedDim)(Random.nextDouble))
    }.cache()
    var messages = curGraph.aggregateMessages(forwardSendMessage, messageCombiner)
    var activeMessages = messages.count()
    var curIteration: Int = 1
    var prevGraph: Graph[Vector, Double] = null
    while (activeMessages > 0 && (curIteration - 1) / 2 < maxIterations) {
      if ((curIteration - 1) % 2 == 0) {
        // Forward messages passing
        val newVerts: VertexRDD[Vector] = curGraph.vertices
          .innerJoin(messages)(VertexProgram).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) =>
          newOpt.getOrElse(old)
        }
        curGraph.cache()
        val oldMessages = messages
        messages = curGraph.aggregateMessages(backSendMessage, messageCombiner).cache()
        oldMessages.unpersist(blocking = false)
        newVerts.unpersist(blocking = false)
      } else {
        //Backward message passing
        val newVerts = curGraph.vertices.innerJoin(messages)(VertexProgram).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        curGraph.cache()
        val oldMessages = messages
        messages = curGraph.aggregateMessages(forwardSendMessage, messageCombiner).cache()
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
