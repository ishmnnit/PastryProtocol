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
  var table = new Array[Array[Int]](log4)
  var numOfBack: Int = 0
  val IDSpace: Int = pow(4, log4).toInt

  var i = 0
  for (i <- 0 until log4)
    table(i) = Array(-1, -1, -1, -1)

  def receive = { 

    case FirstJoin(firstGroup) =>
      firstGroup -= myID

      addBuffer(firstGroup)

      for (i <- 0 until log4) {
        table(i)(toBase4String(myID, log4).charAt(i).toString.toInt) = myID
      }

      sender ! JoinFinish

    case Route(msg, fromID, toID, hops) =>
      if (msg == "Join") {
        var samePre = samePrefix(toBase4String(myID, log4), toBase4String(toID, log4))
        if (hops == -1 && samePre > 0) {
          for (i <- 0 until samePre) {
            context.system.actorSelection("/user/master/" + toID) ! UpdateRow(i, table(i).clone)
          }
        }
        context.system.actorSelection("/user/master/" + toID) ! UpdateRow(samePre, table(samePre).clone)

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
            context.system.actorSelection("/user/master/" + nearest) ! Route(msg, fromID, toID, hops + 1)
          } else { //I am the nearest
            var allLeaf = new ArrayBuffer[Int]()
            allLeaf += myID ++= lessLeaf ++= largerLeaf
            context.system.actorSelection("/user/master/" + toID) ! UpdateLeaf(allLeaf) //Give leaf set info
          }

        } else if (lessLeaf.length < 4 && lessLeaf.length > 0 && toID < lessLeaf.min) {
          context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(msg, fromID, toID, hops + 1)
        } else if (largerLeaf.length < 4 && largerLeaf.length > 0 && toID > largerLeaf.max) {
          context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, fromID, toID, hops + 1)
        } else if ((lessLeaf.length == 0 && toID < myID) || (largerLeaf.length == 0 && toID > myID)) {
          //I am the nearest
          var allLeaf = new ArrayBuffer[Int]()
          allLeaf += myID ++= lessLeaf ++= largerLeaf
          context.system.actorSelection("/user/master/" + toID) ! UpdateLeaf(allLeaf) //Give leaf set info
        } else if (table(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt) != -1) { //Not in leaf set, try routing table
          context.system.actorSelection("/user/master/" + table(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt)) ! Route(msg, fromID, toID, hops + 1)
        } else {
          var diff = IDSpace + 10
          var nearest = -1
          for (i <- 0 until 4) {
            if ((table(samePre)(i) != -1) && (abs(table(samePre)(i) - toID) < diff)) {
              diff = abs(table(samePre)(i) - toID)
              nearest = table(samePre)(i)
            }
          }
          if (nearest != -1) {
            if (nearest == myID) {
              if (toID > myID) { //Not in both
                context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, fromID, toID, hops + 1)
                context.parent ! NotInBoth
              } else if (toID < myID) {
                context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(msg, fromID, toID, hops + 1)
                context.parent ! NotInBoth
              } else {
                println("NO!")
              }
            } else {
              context.system.actorSelection("/user/master/" + nearest) ! Route(msg, fromID, toID, hops + 1)
            }
          }
        }

      } else if (msg == "Route") { //Msg = Route, begin sending message
        //println("From " + fromID + " to " + toID + " I am " + myID)
        //printInfo
        //        if (largerLeaf.length < 4)
        //        println(largerLeaf)
        if (myID == toID) {
          context.parent ! RouteFinish(fromID, toID, hops + 1)
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
              context.system.actorSelection("/user/master/" + nearest) ! Route(msg, fromID, toID, hops + 1)
            } else { //I am the nearest
              context.parent ! RouteFinish(fromID, toID, hops + 1)
            }

          } else if (lessLeaf.length < 4 && lessLeaf.length > 0 && toID < lessLeaf.min) {
            context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(msg, fromID, toID, hops + 1)
          } else if (largerLeaf.length < 4 && largerLeaf.length > 0 && toID > largerLeaf.max) {
            context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, fromID, toID, hops + 1)
          } else if ((lessLeaf.length == 0 && toID < myID) || (largerLeaf.length == 0 && toID > myID)) {
            //I am the nearest
            context.parent ! RouteFinish(fromID, toID, hops + 1)
          } else if (table(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt) != -1) { //Not in leaf set, try routing table
            context.system.actorSelection("/user/master/" + table(samePre)(toBase4String(toID, log4).charAt(samePre).toString.toInt)) ! Route(msg, fromID, toID, hops + 1)
          } else {
            var diff = IDSpace + 10
            var nearest = -1
            for (i <- 0 until 4) {
              if ((table(samePre)(i) != -1) && (abs(table(samePre)(i) - toID) < diff)) {
                diff = abs(table(samePre)(i) - toID)
                nearest = table(samePre)(i)
              }
            }
            if (nearest != -1) {
              if (nearest == myID) {
                if (toID > myID) { //Not in both
                  context.system.actorSelection("/user/master/" + largerLeaf.max) ! Route(msg, fromID, toID, hops + 1)
                  context.parent ! RouteNotInBoth
                } else if (toID < myID) {
                  context.system.actorSelection("/user/master/" + lessLeaf.min) ! Route(msg, fromID, toID, hops + 1)
                  context.parent ! RouteNotInBoth
                }
              } else {
                context.system.actorSelection("/user/master/" + nearest) ! Route(msg, fromID, toID, hops + 1)
              }
            }
          }
        }
      }

    case UpdateRow(rowNum, newRow) =>
      for (i <- 0 until 4)
        if (table(rowNum)(i) == -1)
          table(rowNum)(i) = newRow(i)

    case UpdateLeaf(allLeaf) =>
      addBuffer(allLeaf)
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
          if (table(i)(j) != -1) {
            numOfBack += 1
            context.system.actorSelection("/user/master/" + table(i)(j)) ! UpdateMe(myID)
          }
      }
      for (i <- 0 until log4) {
        table(i)(toBase4String(myID, log4).charAt(i).toString.toInt) = myID
      }

    case UpdateMe(newNodeID) =>
      addOne(newNodeID)
      sender ! Ack

    case Ack =>
      numOfBack -= 1
      if (numOfBack == 0)
        context.parent ! JoinFinish

    case BeginRoute =>
      for (i <- 1 to numRequests)
        context.system.scheduler.scheduleOnce(1000 milliseconds, self, Route("Route", myID, Random.nextInt(IDSpace), -1))
    //self ! Route("Route", myID, Random.nextInt(IDSpace), -1)

    case GoDie =>
      context.system.actorSelection("/user/master/*") ! RemoveMe(myID)
      //println(myID)
      //      for (i <- lessLeaf)
      //        //println(i)
      //        context.system.actorSelection("/user/master/" + i) ! RemoveMe(myID)
      //      for (i <- largerLeaf)
      //        context.system.actorSelection("/user/master/" + i) ! RemoveMe(myID)
      //      for (i <- 0 until log4) {
      //        var j = 0
      //        for (j <- 0 until 4)
      //          if (table(i)(j) != -1)
      //            context.system.actorSelection("/user/master/" + table(i)(j)) ! RemoveMe(myID)
      //      }

      context.stop(self)

    case RemoveMe(theID) =>
      if (theID > myID && largerLeaf.contains(theID)) { //In larger leaf
        largerLeaf -= theID
        if (largerLeaf.length > 0)
          context.system.actorSelection("/user/master/" + largerLeaf.max) ! RequestLeafWithout(theID)
      }
      if (theID < myID && lessLeaf.contains(theID)) { //In less leaf
        lessLeaf -= theID
        if (lessLeaf.length > 0)
          context.system.actorSelection("/user/master/" + lessLeaf.min) ! RequestLeafWithout(theID)
      }
      var samePre = samePrefix(toBase4String(myID, log4), toBase4String(theID, log4))
      if (table(samePre)(toBase4String(theID, log4).charAt(samePre).toString.toInt) == theID) {
        table(samePre)(toBase4String(theID, log4).charAt(samePre).toString.toInt) = -1
        for (i <- 0 until 4) {
          if (table(samePre)(i) != myID && table(samePre)(i) != theID && table(samePre)(i) != -1) {
            context.system.actorSelection("/user/master/" + table(samePre)(i)) !
              RequestInTable(samePre, toBase4String(theID, log4).charAt(samePre).toString.toInt)
          }
        }
      }

    case RequestLeafWithout(theID) =>
      var temp = new ArrayBuffer[Int]()
      temp ++= lessLeaf ++= largerLeaf -= theID
      sender ! LeafRecover(temp.clone, theID)

    case LeafRecover(newlist, theDead) =>
      //if (newlist.contains(theDead)) println("!!!!!!!!!!!")
      for (i <- newlist) {
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
      }

    case RequestInTable(samePre, column) =>
      if (table(samePre)(column) != -1)
        sender ! RecoverRoutingTable(samePre, column, table(samePre)(column))

    case RecoverRoutingTable(row, column, newID) =>
      if (table(row)(column) == -1) {
        table(row)(column) = newID
      }
  }

  
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
      //check routing table
      var samePre = samePrefix(toBase4String(myID, log4), toBase4String(i, log4))
      if (table(samePre)(toBase4String(i, log4).charAt(samePre).toString.toInt) == -1) {
        table(samePre)(toBase4String(i, log4).charAt(samePre).toString.toInt) = i
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
    //check routing table
    var samePre = samePrefix(toBase4String(myID, log4), toBase4String(one, log4))
    if (table(samePre)(toBase4String(one, log4).charAt(samePre).toString.toInt) == -1) {
      table(samePre)(toBase4String(one, log4).charAt(samePre).toString.toInt) = one
    }
  }

}
