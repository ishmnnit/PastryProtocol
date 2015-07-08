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

class PastryActor(numNodes: Int, numRequests: Int, id: Int, log4: Int) extends Actor {

  import context._

  val myID = id;
  var lessLeaf = new ArrayBuffer[Int]()
  var largerLeaf = new ArrayBuffer[Int]()
  var routingTable = new Array[Array[Int]](log4)
  var numOfBack: Int = 0
  val IDSpace: Int = pow(4, log4).toInt

  var i = 0
  for (i <- 0 until log4)
    routingTable(i) = Array(-1, -1, -1, -1)

  def receive = { 

    case PastryInit(firstGroup) =>
      firstGroup -= myID

      addBuffer(firstGroup)

      for (i <- 0 until log4) {
        routingTable(i)(toBase4String(myID, log4).charAt(i).toString.toInt) = myID
      }

      sender ! EndJoin

    case Route(message, fromID, toID, hops) =>
      if (message == "Join") {
        var samePre = samePrefix(toBase4String(myID, log4), toBase4String(toID, log4))
        if (hops == -1 && samePre > 0) {
          for (i <- 0 until samePre) {
            context.system.actorSelection("/user/master/" + toID) ! UpdateRow(i, routingTable(i).clone)
          }
        }
        context.system.actorSelection("/user/master/" + toID) ! UpdateRow(samePre, routingTable(samePre).clone)

        if ((lessLeaf.length > 0 && toID >= lessLeaf.min && toID <= myID) || //In less leaf set
          (largerLeaf.length > 0 && toID <= largerLeaf.max && toID >= myID)) { //In larger leaf set
          var diff = IDSpace + 10
          var nearest = -1
          if (toID < myID) { //In less leaf set
            for (i <- lessLeaf) {
              if (abs(toID - i) < diff) {
                nearest = i
                diff = abs(toID - i)
              }
            }
          } else { //In larger leaf set
            for (i <- largerLeaf) {
              if (abs(toID - i) < diff) {
                nearest = i
                diff = abs(toID - i)
              }
            }
          }

          if (abs(toID - myID) > diff) { // In leaf but not near my id
            context.system.actorSelection("/user/master/" + nearest) ! Route(message, fromID, toID, hops + 1)
          } else { //I am the nearest
            var allLeaf = new ArrayBuffer[Int]()
            allLeaf += myID ++= lessLeaf ++= largerLeaf
            context.system.actorSelection("/user/master/" + toID) ! UpdateLeaf(allLeaf) //Give leaf set info
          }

        } else if (lessLeaf.length < 4 && lessLeaf.length > 0 && toID < lessLeaf.min) {
          context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(message, fromID, toID, hops + 1)
        } else if (largerLeaf.length < 4 && largerLeaf.length > 0 && toID > largerLeaf.max) {
          context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(message, fromID, toID, hops + 1)
        } else if ((lessLeaf.length == 0 && toID < myID) || (largerLeaf.length == 0 && toID > myID)) {
       
          var allLeaf = new ArrayBuffer[Int]()
          allLeaf += myID ++= lessLeaf ++= largerLeaf
          context.system.actorSelection("/user/master/" + toID) ! UpdateLeaf(allLeaf) //Give leaf set info
        } else if (routingTable(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt) != -1) { //Not in leaf set, try routing routingTable
          context.system.actorSelection("/user/master/" + routingTable(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt)) ! Route(message, fromID, toID, hops + 1)
        } else if (toID > myID) { //Not in both
          context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(message, fromID, toID, hops + 1)
          context.parent ! NotInBoth
        } else if (toID < myID) {
          context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(message, fromID, toID, hops + 1)
          context.parent ! NotInBoth
        } else {
          println("Impossible!!!")
        }

      } else if (message == "Route") { //message = Route, begin sending message
        if (myID == toID) {
          context.parent ! EndRoute(fromID, toID, hops + 1)
        } else {
          var samePre = samePrefix(toBase4String(myID, log4), toBase4String(toID, log4))

          if ((lessLeaf.length > 0 && toID >= lessLeaf.min && toID < myID) || //In less leaf set
            (largerLeaf.length > 0 && toID <= largerLeaf.max && toID > myID)) { //In larger leaf set
            var diff = IDSpace + 10
            var nearest = -1
            if (toID < myID) { //In less leaf set
              for (i <- lessLeaf) {
                if (abs(toID - i) < diff) {
                  nearest = i
                  diff = abs(toID - i)
                }
              }
            } else { //In larger leaf set
              for (i <- largerLeaf) {
                if (abs(toID - i) < diff) {
                  nearest = i
                  diff = abs(toID - i)
                }
              }
            }

            if (abs(toID - myID) > diff) { // In leaf but not near my id
              context.system.actorSelection("/user/master/" + nearest) ! Route(message, fromID, toID, hops + 1)
            } else { //I am the nearest
              context.parent ! EndRoute(fromID, toID, hops + 1)
            }

          } else if (lessLeaf.length < 4 && lessLeaf.length > 0 && toID < lessLeaf.min) {
            context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(message, fromID, toID, hops + 1)
          } else if (largerLeaf.length < 4 && largerLeaf.length > 0 && toID > largerLeaf.max) {
            context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(message, fromID, toID, hops + 1)
          } else if ((lessLeaf.length == 0 && toID < myID) || (largerLeaf.length == 0 && toID > myID)) {
            //I am the nearest
            context.parent ! EndRoute(fromID, toID, hops + 1)
          } else if (routingTable(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt) != -1) { //Not in leaf set, try routing routingTable
            context.system.actorSelection("/user/master/" + routingTable(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt)) ! Route(message, fromID, toID, hops + 1)
          } else if (toID > myID) { //Not in both
            context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(message, fromID, toID, hops + 1)
            context.parent ! RouteNotInBoth
          } else if (toID < myID) {
            context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(message, fromID, toID, hops + 1)
            context.parent ! RouteNotInBoth
          } else {
            println("Impossible!!!")
          }
        }
      }

    case UpdateRow(rowNum, newRow) =>
      for (i <- 0 until 4)
        if (routingTable(rowNum)(i) == -1)
          routingTable(rowNum)(i) = newRow(i)

    case UpdateLeaf(allLeaf) =>
      addBuffer(allLeaf)
      //printInfo
      for (i <- lessLeaf) {
        numOfBack += 1
        context.system.actorSelection("/user/master/" + i) ! UpdateMe(myID)
      }
      for (i <- largerLeaf) {
        numOfBack += 1
        context.system.actorSelection("/user/master/" + i) ! UpdateMe(myID)
      }
      for (i <- 0 until log4) {
        var j = 0
        for (j <- 0 until 4)
          if (routingTable(i)(j) != -1) {
            numOfBack += 1
            context.system.actorSelection("/user/master/" + routingTable(i)(j)) ! UpdateMe(myID)
          }
      }
      for (i <- 0 until log4) {
        routingTable(i)(toBase4String(myID, log4).charAt(i).toString.toInt) = myID
      }

    case UpdateMe(newNodeID) =>
      addOne(newNodeID)
      sender ! Ack

    case Ack =>
      numOfBack -= 1
      if (numOfBack == 0)
        context.parent ! EndJoin


    case BeginRoute =>
      for (i <- 1 to numRequests)
        context.system.scheduler.scheduleOnce(1000 milliseconds, self, Route("Route", myID, Random.nextInt(IDSpace), -1))
    //self ! Route("Route", myID, Random.nextInt(IDSpace), -1)

  }

  //Functions of Pastry Nodes-------------------------------------------------------------------------------------------------------------------------
  def toBase4String(raw: Int, length: Int): String = {
    var str: String = Integer.toString(raw, 4)
    val diff: Int = length - str.length()
    if (diff > 0) {
      var j = 0
      while (j < diff) {
        str = '0' + str
        j += 1
      }
    }
    return str
  }

  def samePrefix(str1: String, str2: String): Int = {
    var j = 0
    while (j < str1.length && str1.charAt(j).equals(str2.charAt(j))) {
      j += 1
    }
    return j
  }

  def addBuffer(all: ArrayBuffer[Int]): Unit = {
    for (i <- all) {
      if (i > myID && !largerLeaf.contains(i)) { //i may be added to larger leaf
        if (largerLeaf.length < 4) {
          largerLeaf += i
        } else {
          if (i < largerLeaf.max) {
            largerLeaf -= largerLeaf.max
            largerLeaf += i
          }
        }
      } else if (i < myID && !lessLeaf.contains(i)) { //i may be added to less leaf
        if (lessLeaf.length < 4) {
          lessLeaf += i
        } else {
          if (i > lessLeaf.min) {
            lessLeaf -= lessLeaf.min
            lessLeaf += i
          }
        }
      }
      //check routing routingTable
      var samePre = samePrefix(toBase4String(myID, log4), toBase4String(i, log4))
      if (routingTable(samePre)(toBase4String(i, log4).charAt(samePre).toString.toInt) == -1) {
        routingTable(samePre)(toBase4String(i, log4).charAt(samePre).toString.toInt) = i
      }
    }
  }

  def addOne(one: Int): Unit = {
    if (one > myID && !largerLeaf.contains(one)) { //i may be added to larger leaf
      if (largerLeaf.length < 4) {
        largerLeaf += one
      } else {
        if (one < largerLeaf.max) {
          largerLeaf -= largerLeaf.max
          largerLeaf += one
        }
      }
    } else if (one < myID && !lessLeaf.contains(one)) { //i may be added to less leaf
      if (lessLeaf.length < 4) {
        lessLeaf += one
      } else {
        if (one > lessLeaf.min) {
          lessLeaf -= lessLeaf.min
          lessLeaf += one
        }
      }
    }
    //check routing routingTable
    var samePre = samePrefix(toBase4String(myID, log4), toBase4String(one, log4))
    if (routingTable(samePre)(toBase4String(one, log4).charAt(samePre).toString.toInt) == -1) {
      routingTable(samePre)(toBase4String(one, log4).charAt(samePre).toString.toInt) = one
    }
  }

}