package pastry

import akka.actor._
import scala.math._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration._
import scala.Char._
import scala.language.postfixOps
import java.util.concurrent.TimeUnit

class Master(numNodes: Int, numRequests: Int) extends Actor {

  var log4 = ceil(log(numNodes.toDouble) / log(4)).toInt
  var nodeIDSpace: Int = pow(4, log4).toInt
  var nodeList = new ArrayBuffer[Int]()
  var firstGroup = new ArrayBuffer[Int]()
  var numFirstGroup: Int = if (numNodes <= 1024) numNodes else 1024 //Default first group size, can be changed later
  var i: Int = -1
  var numJoined: Int = 0
  var numNotInBoth: Int = 0
  var numRouted: Int = 0
  var numHops: Int = 0
  var numRouteNotInBoth: Int = 0

  println("Number Of Nodes: " + numNodes)
  println("Number Of Request Per Node: " + numRequests)

  for (i <- 0 until nodeIDSpace) { //Node space form 0 to node id space
    nodeList += i
  }
  nodeList = Random.shuffle(nodeList) //Random list index from 0 to nodes-2 there is no node 0!

  for (i <- 0 until numFirstGroup) {
    firstGroup += nodeList(i)
  }
 

  for (i <- 0 until numNodes) {
    context.actorOf(Props(new PastryActor(numNodes, numRequests, nodeList(i), log4)), name = String.valueOf(nodeList(i))) //Create nodes
  }

  def receive = { 
    case Go =>
      println("Join Begins...")
      for (i <- 0 until numFirstGroup)
        context.system.actorSelection("/user/master/" + nodeList(i)) ! PastryInit(firstGroup.clone)

    case EndJoin =>
      numJoined += 1
      if (numJoined == numFirstGroup) {
        if (numJoined >= numNodes) {
          self ! BeginRoute
        } else {
          self ! NextJoin
        }
      }

      if (numJoined > numFirstGroup) {
        if (numJoined == numNodes) {
          self ! BeginRoute
        } else {
          self ! NextJoin
        }

      }

    case NextJoin =>
      val startID = nodeList(Random.nextInt(numJoined))
      context.system.actorSelection("/user/master/" + startID) ! Route("Join", startID, nodeList(numJoined), -1)

    case BeginRoute =>
	  println("Join Finished!")
      println("Routing Starts")
      context.system.actorSelection("/user/master/*") ! BeginRoute

    case NotInBoth =>
      numNotInBoth += 1

    case EndRoute(fromID, toID, hops) =>
      numRouted += 1
      numHops += hops
      
       if (numRouted % 1000 == 0)
          println(numRouted + " Message routed")

      if (numRouted >= numNodes * numRequests) {
        println("Number of Total Routes: " + numRouted)
        println("Number of Total Hops: " + numHops)
        println("Average Hops Per Route: " + numHops.toDouble / numRouted.toDouble)
        context.system.shutdown()
      }

    case RouteNotInBoth =>
      numRouteNotInBoth += 1
  }

}