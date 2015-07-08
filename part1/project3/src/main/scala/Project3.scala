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

case object Go
case object Test
case object Start
case object GoJoin
case object BeginRoute
case object NextJoin
case object RequestLeaf
case object Ack
case object EndJoin
case object Report
case object NotInBoth
case object RouteNotInBoth
case class Route(msg: String, fromID: Int, toID: Int, hops: Int)
case class PastryInit(firstEight: ArrayBuffer[Int])
case class UpdateRow(rowNum: Int, row: Array[Int])
case class UpdateLeaf(allLeaf: ArrayBuffer[Int])
case class UpdateMe(newNodeID: Int)
case class EndRoute(fromID: Int, toID: Int, hops: Int)

object Project3 {
  def main(args: Array[String]) {

    if (args.length != 2) {
      println("No Argument(s)! Using default mode:")
      pastry(numNodes = 1000, numRequests = 10) //Default mode
    } else {
      pastry(numNodes = args(0).toInt, numRequests = args(1).toInt) //User Specified Mode
    }

    def pastry(numNodes: Int, numRequests: Int) {
      val system = ActorSystem("pastry")
      val master = system.actorOf(Props(new Master(numNodes, numRequests)), name = "master")
      master ! Go
    }
  }
}

