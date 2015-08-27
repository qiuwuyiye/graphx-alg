package pagerank

/**
 * Created by liping on 8/27/15.
 */


import org.apache.spark.graphx._
import scala.reflect.ClassTag


/**
 * Graph pagerank Algorithm Implementation.
 *
 * Implementation based on Pregel.
 * Assuming the degree's reciprocal matrix is A(n * n), and the vector of weight is x
 * Update x with x_new = A * x_old * (1 - reset_prob) + reset_prob * x_old.
 * When the iteration count get to the maxIter, or the x_new is approximate to x_old, the iteration converges
 * As to this, the whole algorithm halt.
 */
object IctPageRank {

  def run[VD: ClassTag, ED: ClassTag, A: ClassTag](graph: Graph[VD, Double],
                                                   maxIter: Int = 10,
                                                   reset_prob: Double = 0.15,
                                                   tol: Double = 0.0001): Graph[(Double, Double), Double] = {

    /**
     * Update the graph vertex attribute
     * @param id  Vertex_id
     * @param attr  vertex_attribute
     * @param msg_sum  the combined messages from other messages
     * @return  A new vertex attribute with its pagerank value and delta
     */
    def updateVertex(id: VertexId, attr: (Double, Double), msg_sum: Double): (Double, Double) = {
      val (oldPR, lastdelta) = attr
      val newPR = oldPR * reset_prob + (1.0 - reset_prob) * msg_sum
      (newPR, math.abs(newPR - oldPR))
    }

    /**
     * Send messages to the destination vetex with edge direction
     * @param triplet EdgeContext[vertex_attribute, edge_attribute, message]
     */
    def sendMessage(triplet: EdgeContext[(Double, Double), Double, Double]) = {
      if (triplet.srcAttr._2 > tol) {
        triplet.sendToDst(triplet.srcAttr._1 * triplet.attr)
      }
    }


    def messageCombiner(a: Double, b: Double): Double = a + b

    //Initial the graph with each edge attribute having weight 1/outDegree and vertex with attribute 1.0
    var g: Graph[(Double, Double), Double] = graph.outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // set the vertex attribute to (initial PR, delta)
      .mapVertices((id, attr) => (1.0, 1.0))
      .cache()

    //compute the messages
    var messages = g.aggregateMessages(sendMessage, messageCombiner).cache()

    var activeMessages = messages.count()
    //Loop
    var prevG: Graph[(Double, Double), Double] = null
    var i = 0
    while (activeMessages > 0 && i < maxIter) {

      //Receive the messages. Vertices that didn't get any messages do not appear in new_verts.
      val new_verts = g.vertices.innerJoin(messages)(updateVertex).cache()
      //Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(new_verts) { (vid, old, newopt) => newopt.getOrElse(old) }
      g.cache()
      val old_messages = messages
      //Send new messages, Vertices that didn't get any messages don't appear in newVerts and they will not send messages
      //We mush cache messages, so it can be materialized on the next line, allowing us the uncache the previous iteration.
      messages = g.aggregateMessages(sendMessage, messageCombiner).cache()
      activeMessages = messages.count()
      old_messages.unpersist(blocking = false)
      new_verts.unpersist(blocking = false)
      prevG.unpersist(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i += 1
    }
    g
  }
}


