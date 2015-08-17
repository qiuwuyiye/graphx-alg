package NMF

import org.apache.spark.graphx._
//import org.apache.spark.mllib.linalg.Vector
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark.util.Vector


/**
 * Graph NMF algorithm implementation.
 *
 * Implementation Idea based on Pregel
 *      D = WH
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
 * W_i <- (1-learn_rate*reg)*W_i + learn_rate sigma{ (d_ij - W_i*H_j) * H_j }
 * H_j <- (1-learn_rate*reg)*H_j + learn_rate sigma{ (d_ij - W_i*H_j) * W_i }
 *
 * `learn_rate` is the learning rate
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
   * @param maxIter the max iteration
   * @param learn_rate the learn rate
   * @param reg the normalization item
   * @param K the reduced Dimension in NMF algorithm
   *
   * @return the graph containing the two Vectors which is Vector W and Vector H
   *         and edge attributes containing the edge weight.
   *
   */
  def run[VD: ClassTag](graph: Graph[VD, Double],
    maxIter: Int = 10,
    learn_rate: Double = 0.01,
    reg: Double = 0.1,
    K: Int = 50) = {

    /**
     * Update graph vertex attribute function.
     *
     * @param id  Vertex id
     * @param attri  Vertex attribute
     * @param msgSum The combined message sum from other vertex
     * @return A new vector and it's the vertex's new attribute
     */
    def updateVertex(id: VertexId, attri: Vector, msgSum: Vector): Vector = {
      val scale = 1 - learn_rate * reg
      val intercept = learn_rate * msgSum
      val newV: Vector = scale * attri + intercept
      // set the negative value to zero
      Vector(newV.elements.map(math.max(_, 0)))
    }

    /**
     * Send messages to the source vertex with the opposite edge direction
     *
     * @param triplet [vertex attribute, edge attribute, message]
     */
    def sendDeltaOfW(triplet: EdgeContext[Vector, Double, Vector]) {
      triplet.sendToSrc((triplet.attr - triplet.srcAttr.dot(triplet.dstAttr)) * triplet.dstAttr)
    }

    /**
     * Send messages to the destination vertex with edge direction
     *
     * @param triplet [vertex attribute, edge attribute, message]
     */
    def sendDeltaOfH(triplet: EdgeContext[Vector, Double, Vector]) {
      triplet.sendToDst((triplet.attr - triplet.srcAttr.dot(triplet.dstAttr)) * triplet.srcAttr)
    }

    /**
     * Combine the two messages and return the sum of them.
     * @param a A vector
     * @param b A vector
     * @return  Sum of a and b
     */
    def messageCombiner(a: Vector, b: Vector): Vector = a + b

    // Initiate each Vertex's vector W and vector H whose dimension is K
    var curGraph: Graph[Vector, Double] = graph.mapVertices { (vid, vdata) =>
      Vector(Array.fill(K)(Random.nextDouble))
    }.cache()


    var delta: VertexRDD[Vector] = curGraph.aggregateMessages(sendDeltaOfW, messageCombiner).cache()

    // number of vertexes updated
    var n_update = delta.count()
    var iter: Int = 1
    var prevGraph: Graph[Vector, Double] = null
    while (n_update > 0 && ((iter - 1) / 2 < maxIter)) {
      if ((iter - 1) % 2 == 0) {
        // Forward messages passing
        val new_W: VertexRDD[Vector] = curGraph.vertices
          .innerJoin(delta)(updateVertex).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(new_W) { (vid, old, newOpt) =>
          newOpt.getOrElse(old)
        }
        curGraph.cache()
        val old_delta = delta
        delta = curGraph.aggregateMessages(sendDeltaOfH, messageCombiner).cache()
        old_delta.unpersist(blocking = false)
        new_W.unpersist(blocking = false)
      } else {
        //Backward message passing
        val new_H = curGraph.vertices.innerJoin(delta)(updateVertex).cache()
        prevGraph = curGraph
        curGraph = curGraph.outerJoinVertices(new_H) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        curGraph.cache()
        val old_delta = delta
        delta = curGraph.aggregateMessages(sendDeltaOfW, messageCombiner).cache()
        old_delta.unpersist(blocking = false)
        new_H.unpersist(blocking = false)
      }
      n_update = delta.count()
      prevGraph.unpersistVertices(blocking = false)
      prevGraph.edges.unpersist(blocking = false)
      iter += 1
    }
    curGraph
  }
}
