package com.cs553.bloom

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import com.cs553.bloom.ApplcationConstants._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.TreeMap
import scala.util.hashing.{MurmurHash3 => mmh3}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Random, Success}

/*
*
* Created by: prajw
* Date: 09-Nov-20
*
*/
object ProcessActor extends LazyLogging {

  sealed trait ProcessMessages
  // Messages Process responds to That comes from a Process Actor (can be self)
  case object ExecuteSomething extends ProcessMessages
  case class SendResponse(gsn: GsnValue) extends ProcessMessages
  case class RecvResponse(gsn: GsnValue) extends ProcessMessages
  case class InternalResponse(gsn: GsnValue) extends ProcessMessages

  // To find other Processes
  private case class ListingResponse(listing: Receptionist.Listing) extends ProcessMessages

  //Initialize process to get other Processes' reference
  final case object InitProcess extends ProcessMessages

  // Main Messages that define the Actor's jobs. These messages come from the simulator
  final case class SendMessage(whom: ActorRef[ProcessMessages]) extends ProcessMessages
  final case class RecvMessage(vectorClock: List[Int],
                               bloomClock: List[Int]) extends ProcessMessages

  final case object InternalEvent extends ProcessMessages
  final case object ShowInternals extends ProcessMessages


  // Adapted Responses. Process get the message from the guard and Marshals and sends back to itself to the future
  // These are blocking. Meaning, a process wont execute any event until it receives these responses
  private case class ReceivedSendGSN(gsnValue: GsnValue,
                                     whom: ActorRef[ProcessMessages]) extends ProcessMessages

  private case class ReceivedReceiveGSN(gsnValue: GsnValue,
                                        senderVectorClock: List[Int],
                                        senderBloomClock: List[Int]) extends ProcessMessages

  private case class ReceivedInternalGSN(gsnValue: GsnValue) extends ProcessMessages


  val probabilities: Map[Double, String] = TreeMap(0.65 -> "None", 0.35 -> "Send", 0.0 ->"Internal")

  val ProcessKey: ServiceKey[ProcessMessages] = ServiceKey(s"Process")
  def apply(numProcesses: Int,
            processID: Int,
            guardRef: ActorRef[GuardActor.Command]): Behavior[ProcessMessages] = {
    val bloomClock: List[Int] = List.fill(Math.ceil(numProcesses * BLOOM_CLOCK_LENGTH_RATIO).toInt)(0)
    val vectorClock: List[Int] = List.fill(numProcesses)(0)
    logger.info(s"Process Starting: $processID")
    Behaviors.setup { context: ActorContext[ProcessMessages] =>
      context.system.receptionist ! Receptionist.Register(ProcessKey, context.self)
      new ProcessActor(context).idleProcess(guardRef, processID, bloomClock, vectorClock, 0, None)
    }
  }
}

class ProcessActor(context: ActorContext[ProcessActor.ProcessMessages]) {

  import ProcessActor._


  val listingAdapter: ActorRef[Receptionist.Listing] =
    context.messageAdapter { listing =>
      println(s"listingAdapter:listing: ${listing.toString}")
      ListingResponse(listing)
    }

  def idleProcess(guardActor: ActorRef[GuardActor.Command],
                  processID: Int,
                  bloomClock: List[Int],
                  vectorClock: List[Int],
                  eventCounter: Int,
                  processRefs: Option[Set[ActorRef[ProcessMessages]]] = None): Behavior[ProcessMessages] = {

    Behaviors.receiveMessagePartial{
      case InitProcess =>
        context.log.debug("Find the Other process")
        context.system.receptionist ! Receptionist.Find(ProcessKey, listingAdapter)
        Behaviors.same

      case ListingResponse(ProcessKey.Listing(listings)) =>
        context.log.debug("Got Listing of {}", listings)
        idleProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, Some(listings - context.self))


      case ExecuteSomething =>
        context.log.debug("Executing Something")
        val res = randomExec(probabilities)
        implicit val timeout: Timeout = Timeout(1.seconds)
        res.toLowerCase match {

          case "send" =>
            context.log.debug("Ready for sending")
            val whom = randomEle(processRefs.get)
            context.ask(guardActor, GuardActor.IncrementAndReplySender) {
              case Success(SendResponse(message)) => ReceivedSendGSN(message, whom)
              case Failure(_) => ReceivedSendGSN(GsnValue(-1), whom)
            }
            executingProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, processRefs)


          case "internal" =>
            context.log.debug("Ready for internal execution")
            context.ask(guardActor, GuardActor.IncrementAndReplyInternal) {
              case Success(InternalResponse(message)) => ReceivedInternalGSN(message)
              case Failure(_) => ReceivedInternalGSN(GsnValue(-1))
            }
            executingProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, processRefs)

          case _ =>
            context.log.debug("Not doing anything")
            Behaviors.same
        }


      case RecvMessage(senderVectorClock, senderBloomCLock) =>
        context.log.debug("Received a message")
        implicit val timeout: Timeout = Timeout(1.seconds)
        context.ask(guardActor, GuardActor.IncrementAndReplyReceiver) {
          case Success(RecvResponse(message)) => ReceivedReceiveGSN(message, senderVectorClock, senderBloomCLock)
          case Failure(_) => ReceivedReceiveGSN(GsnValue(-1), senderVectorClock, senderBloomCLock)
        }
        executingProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, processRefs)

      case ShowInternals =>
        context.log.info(s"dumping: Process $processID, latest VC: $vectorClock, latestBC: $bloomClock, latestExecution: $eventCounter")
        Behaviors.same


    }


  }

  def executingProcess(guardActor: ActorRef[GuardActor.Command],
                       processID: Int,
                       bloomClock: List[Int],
                       vectorClock: List[Int],
                       eventCounter: Int,
                       processRefs: Option[Set[ActorRef[ProcessMessages]]]): Behavior[ProcessMessages] = {
    Behaviors.receiveMessagePartial {


      case ReceivedSendGSN(gsnValue, whom) =>
        context.log.debug(s"Received GSN for a send event of ${gsnValue.gsn}")
        if (gsnValue.gsn != -1) {
          val newEventCounter = eventCounter + 1
          context.log.debug(s"Incrementing Process Event (send) counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, RULE_1)
          val newBC = performBCRules(bloomClock, processID, newEventCounter, SEND_EVENT)
          context.log.debug(s"Sending Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
          whom ! RecvMessage(newVC, newBC)
          idleProcess(guardActor, processID, newBC, newVC, newEventCounter, processRefs)
        } else {
          idleProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, processRefs)
        }


      case ReceivedReceiveGSN(gsnValue, receivedVectorClock, receivedBloomClock) =>
        context.log.debug(s"Received GSN for receive event of ${gsnValue.gsn}")
        if (gsnValue.gsn != -1) {
          val newEventCounter = eventCounter + 1
          context.log.debug(s"Incrementing process Event (receive) counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, RULE_2, receivedVectorClock)
          val newBC = performBCRules(bloomClock, processID, newEventCounter, RECV_EVENT, receivedBloomClock)
          context.log.debug(s"recv Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
          idleProcess(guardActor, processID, newBC, newVC, newEventCounter, processRefs)
        }
        else
          idleProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, processRefs)


      case ReceivedInternalGSN(gsnValue) =>
        context.log.debug(s"Received GSN for an Internal event of ${gsnValue.gsn}")
        if (gsnValue.gsn != -1) {
          val newEventCounter = eventCounter + 1
          context.log.debug(s"Incrementing Process Event (Internal) counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, RULE_1)
          val newBC = performBCRules(bloomClock, processID, newEventCounter, INTERNAL_EVENT)
          context.log.debug(s"Internal Event Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
          idleProcess(guardActor, processID, newBC, newVC, newEventCounter, processRefs)
        } else {
          idleProcess(guardActor, processID, bloomClock, vectorClock, eventCounter, processRefs)
        }


    }


  }

  // Utility functions
  def randomExec(probs: Map[Double, String]): String = {
    val rand = Random.nextDouble()
    val ranges = probs.tail.scanLeft(probs.head) {
      case ((prob1, _), (prob2, value)) => (prob1 + prob2, value)
    }
    val res =  ranges.dropWhile(_._1 < rand).map(_._2).headOption.get
    res
  }

  def randomEle[T](s: Set[T]): T = {
    val n = Random.nextInt(s.size)
    s.iterator.drop(n).next()
  }


  def performVCRules(vectorClock: List[Int],
                     processID: Int,
                     ruleID: Int,
                     receiptVectorClock: List[Int] = List.empty): List[Int] = {
    ruleID match {
      // Rule 1
      case RULE_1 =>
        // on sending message or an internal event
        val newVC = vectorClock.updated(processID, vectorClock(processID) + 1)
        newVC

      // Rule 2
      case RULE_2 =>
        // on receipt of message
        // get the max of both the VCs
        val maxVC = vectorClock.zip(receiptVectorClock).map(x => x._1.max(x._2))
        // execute R1
        val newVC = maxVC.updated(processID, maxVC(processID) + 1)
        newVC
    }
  }

  def performBCRules(bloomClock: List[Int],
                     i: Int, x: Int,
                     ruleID: Int,
                     bloomClockReceived: List[Int] = List.empty): List[Int] = {
    val m = bloomClock.length
    val hashSeeds = List(SEED_1, SEED_2, SEED_3, SEED_4)
    val hashing_tuple = (i, x)
    val hashValues = (1 to K).map(k => mmh3.productHash(hashing_tuple, hashSeeds(k))).toList
    val bloomCounters = hashValues.map(hashValue => Math.abs(hashValue % m))
    ruleID match {
      case INTERNAL_EVENT =>
        val newBloomClock = bloomClock.toArray
        bloomCounters.foreach(i => newBloomClock(i) += 1)
        newBloomClock.toList
      case SEND_EVENT =>
        val newBloomClock = bloomClock.toArray
        bloomCounters.foreach(i => newBloomClock(i) += 1)
        newBloomClock.toList
      case RECV_EVENT =>
        val newBloomClock: Array[Int] = bloomClock.zip(bloomClockReceived).map(x => x._1.max(x._2)).toArray
        bloomCounters.foreach(i => newBloomClock(i) += 1)
        newBloomClock.toList
    }
  }


}
