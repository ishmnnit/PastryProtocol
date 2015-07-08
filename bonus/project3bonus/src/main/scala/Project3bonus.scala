
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
case object Start
case object GoJoin
case object BeginRoute
case object NextJoin
case object RequestLeaf
case object Ack
case object JoinFinish
case object Report
case object NotInBoth
case object RouteNotInBoth
case object CreateFailures
case object GoDie
case class Route(msg: String, fromID: Int, toID: Int, hops: Int)
case class FirstJoin(firstEight: ArrayBuffer[Int])
case class UpdateRow(rowNum: Int, row: Array[Int])
case class UpdateLeaf(allLeaf: ArrayBuffer[Int])
case class UpdateMe(newNodeID: Int)
case class RouteFinish(fromID: Int, toID: Int, hops: Int)
case class RemoveMe(theID: Int)
case class RequestLeafWithout(theID: Int)
case class LeafRecover(newlist: ArrayBuffer[Int], theDead: Int)
case class RequestInTable(row: Int, column: Int)
case class RecoverRoutingTable(row: Int, column: Int, newID: Int)

object project3bonus {
  def main(args: Array[String]) {

    if (args.length == 3) {
      pastry(numNodes = args(0).toInt, numRequests = args(1).toInt, numFailures = args(2).toInt) 
    } else if (args.length == 2) {
      pastry(numNodes = args(0).toInt, numRequests = args(1).toInt, numFailures = args(0).toInt / 100)
    } else {
      println("Nor Enough Argument ")
      
    }

    def pastry(numNodes: Int, numRequests: Int, numFailures: Int) {
      val system = ActorSystem("pastry")
      val master = system.actorOf(Props(new Master(numNodes, numRequests, numFailures)), name = "master")
      master ! Go
    }
  }
}

