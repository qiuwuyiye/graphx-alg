package pagerank

/**
 * Created by liping on 8/27/15.
 */


import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.graphx.lib.PageRank


/**
 * Graph pagerank Algorithm Implementation.
 *
 * Implementation based on Pregel.
 * Assuming the degree's reciprocal matrix is A(n * n), and the vector of weight is x
 * Update x with x_new = A * x_old * (1 - reset_prob) + reset_prob * sum(x_old) / x_count.
 * This is the pow method.
 * When the iteration count get to the maxIter, or the x_new is approximate to x_old, the iteration converges
 * As to this, the whole algorithm halt.
 */
object IctPageRank {

  def run[VD: ClassTag, ED: ClassTag, A: ClassTag](graph: Graph[Double, Double],
                                                   maxIter: Int = 10,
                                                   reset_prob: Double = 0.15,
                                                   tol: Double = 0.0001):Graph[Double, Double] = {

    /**
     * Update the graph vertex attribute
     * @param id  Vertex_id
     * @param attr  vertex_attribute
     * @param msg_sum  the combined messages from other messages
     * @return  A new vertex attribute with its pagerank value and delta
     */

    def updateVertex(id: VertexId, attr: Double, msg_sum: Option[Double]): Double = {
       (1.0 - reset_prob) * msg_sum.getOrElse(0.0)
    }


    /**
     * Send messages to the destination vetex with edge direction
     * @param triplet EdgeContext[vertex_attribute, edge_attribute, message]
     */
    def sendMessage(triplet: EdgeContext[Double, Double, Double]) = {
      triplet.sendToDst(triplet.srcAttr * triplet.attr)
    }


    def messageCombiner(a: Double, b: Double): Double = a + b

    //Initial the graph with each edge attribute having weight 1/outDegree and vertex with attribute 1.0
    val g: Graph[Double, Double] = graph.outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // set the vertex attribute to (initial PR, delta)
      .mapVertices((id, attr) => 1.0)
      .cache()

    val vertices_num = g.vertices.count()
    val result: Graph[Double, Double] = pregel_pagerank(g, maxIter, tol, vertices_num, reset_prob)(updateVertex, sendMessage, messageCombiner)
    val sum: Double = result.vertices.map(f => f._2).reduce(_ + _)
    result.mapVertices((vid, vdata) => vdata / sum)
  }

  def pregel_pagerank(graph:Graph[Double, Double], maxIter: Int = Int.MaxValue, tol: Double, vertices_num: Long, reset_prob: Double)
                     (vprog: (VertexId, Double, Option[Double]) => Double,
                      sendMsg: EdgeContext[Double, Double, Double] => Unit,
                      mergeMsg:(Double, Double) => Double ): Graph[Double, Double] =
  {
    def vertex_update(id: VertexId, attr: Double, add_PR: Option[Double]): Double=
      attr + add_PR.getOrElse(0.0)
    var g = graph.cache()
    var PR_avg = g.vertices.map(f => f._2).reduce(_ + _) / vertices_num

    //compute the messages
    var messages = g.aggregateMessages(sendMsg, mergeMsg).cache()

    //Loop
    var prevG: Graph[Double, Double] = null
    var i = 0
    var is_break = true
    while (is_break && i < maxIter) {
      //Receive the messages. Vertices that didn't get any messages do not appear in new_verts.
      prevG = g
      //graph_old keep the old vertices's attribute
      val graph_old = g.vertices.cache()
      val new_verts = g.mapVertices((id, attr) =>{
        val add_PR:Double = reset_prob * PR_avg
        add_PR
      }).vertices.cache()
      //Update the graph with the new vertices attribute (Here it is the message received from neighbours.
      g = g.outerJoinVertices(messages)(vprog)
      //Update the graph with the vertices attribute added a damping factor 0.15 * PR(sum) / vertices_count
      g = g.outerJoinVertices(new_verts)(vertex_update)
      PR_avg = g.vertices.map(f => f._2).reduce(_ + _) / vertices_num
      g.cache()
      //graph_new keeps the new vertices attribute
      val graph_new = g.vertices.cache()
      val old_messages = messages
      messages = g.aggregateMessages(sendMsg, mergeMsg).cache()
      old_messages.unpersist(blocking = false)
      new_verts.unpersist(blocking = false)
      prevG.unpersist(blocking = false)
      prevG.edges.unpersist(blocking = false)
      i += 1
      //Calculate the PR error sum of all vertices if the sum is less than tol, then the PageRank alg converges
      val error: Double = graph_new.innerJoin(graph_old)((id, vd1, vd2) => math.abs(vd1 - vd2)).map(f => f._2).reduce(_ + _)
      graph_old.unpersist(blocking = false)
      graph_new.unpersist(blocking = false)
      if (error < tol)
        {
          is_break = false
        }
    }
    g
  }
}


