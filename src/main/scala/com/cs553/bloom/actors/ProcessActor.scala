package com.cs553.bloom.actors

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import com.cs553.bloom.utils.ApplicationConstants._
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

  val probabilities: Map[Double, String] = TreeMap(1.0 -> "Send", 0.0 -> "Internal")
  val probabilitiesNormalized: Map[Double, String] = probabilities.map { case (k, v) =>
    k / probabilities.keys.sum.toDouble -> v
  }
  val finalProbabilities: Map[Double, String] = TreeMap(probabilitiesNormalized.toArray:_*)
  val ProcessKey: ServiceKey[ProcessMessages] = ServiceKey(s"Process")

  def apply(numProcesses: Int,
            K: Int,
            M: Int,
            processID: Int,
            guardRef: ActorRef[GuardActor.Command],
            writerRef: ActorRef[LogWriter.WriterMessages]): Behavior[ProcessMessages] = {

    val bloomClock: List[Int] = List.fill(M)(0)
    val vectorClock: List[Int] = List.fill(numProcesses)(0)
    logger.info(s"Process Starting: $processID")
    Behaviors.setup { context: ActorContext[ProcessMessages] =>
      context.system.receptionist ! Receptionist.Register(ProcessKey, context.self)
      new ProcessActor(context, guardRef, writerRef, numProcesses, K, M)
        .idleProcess(processID, bloomClock, vectorClock, 0, None)
    }

  }

  sealed trait ProcessMessages

  case class SendResponse(gsn: GsnValue) extends ProcessMessages

  case class RecvResponse(gsn: GsnValue) extends ProcessMessages

  case class InternalResponse(gsn: GsnValue) extends ProcessMessages

  // Main Messages that define the Actor's jobs. These messages come from the simulator
  final case class SendMessage(whom: ActorRef[ProcessMessages]) extends ProcessMessages

  final case class RecvMessage(vectorClock: List[Int],
                               bloomClock: List[Int]) extends ProcessMessages

  // To find other Processes
  private case class ListingResponse(listing: Receptionist.Listing) extends ProcessMessages

  // Adapted Responses. Process get the message from the guard and Marshals and sends back to itself to the future
  // These are blocking. Meaning, a process wont execute any event until it receives these responses
  private case class ReceivedSendGSN(gsnValue: GsnValue,
                                     whom: ActorRef[ProcessMessages]) extends ProcessMessages

  private case class ReceivedReceiveGSN(gsnValue: GsnValue,
                                        senderVectorClock: List[Int],
                                        senderBloomClock: List[Int]) extends ProcessMessages

  private case class ReceivedInternalGSN(gsnValue: GsnValue) extends ProcessMessages

  // Messages Process responds to That comes from a Process Actor (can be self)
  case object ExecuteSomething extends ProcessMessages

  //Initialize process to get other Processes' reference
  final case object InitProcess extends ProcessMessages

  final case object InternalEvent extends ProcessMessages

  final case object ShowInternals extends ProcessMessages
}

class ProcessActor(context: ActorContext[ProcessActor.ProcessMessages],
                   guardActor: ActorRef[GuardActor.Command],
                   writingActor: ActorRef[LogWriter.WriterMessages],
                   numProcesses: Int,
                   K: Int,
                   M: Int) {

  import ProcessActor._
  import LogWriter.WriteToFile

  val listingAdapter: ActorRef[Receptionist.Listing] =
    context.messageAdapter { listing =>
      context.log.debug(s"listingAdapter:listing: ${listing.toString}")
      ListingResponse(listing)
    }

  def idleProcess(processID: Int,
                  bloomClock: List[Int],
                  vectorClock: List[Int],
                  eventCounter: Int,
                  processRefs: Option[Set[ActorRef[ProcessMessages]]] = None): Behavior[ProcessMessages] = {

    Behaviors.receiveMessagePartial {
      case InitProcess =>
        context.log.debug("Find the Other process")
        context.system.receptionist ! Receptionist.Find(ProcessKey, listingAdapter)
        Behaviors.same

      case ListingResponse(ProcessKey.Listing(listings)) =>
        context.log.debug("Got Listing of {}", listings)
        idleProcess(processID, bloomClock, vectorClock, eventCounter, Some(listings - context.self))


      case ExecuteSomething =>
        context.log.debug("Executing Something")
        val res = randomExec(finalProbabilities)
        implicit val timeout: Timeout = Timeout(1.seconds)
        res.toLowerCase match {

          case "send" =>
            context.log.debug("Ready for sending")
            val whom = randomEle(processRefs.get)
            context.ask(guardActor, GuardActor.IncrementAndReplySender) {
              case Success(SendResponse(message)) => ReceivedSendGSN(message, whom)
              case Failure(_) => ReceivedSendGSN(GsnValue(-1), whom)
            }
            executingProcess(processID, bloomClock, vectorClock, eventCounter, processRefs)


          case "internal" =>
            context.log.debug("Ready for internal execution")
            context.ask(guardActor, GuardActor.IncrementAndReplyInternal) {
              case Success(InternalResponse(message)) => ReceivedInternalGSN(message)
              case Failure(_) => ReceivedInternalGSN(GsnValue(-1))
            }
            executingProcess(processID, bloomClock, vectorClock, eventCounter, processRefs)

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
        executingProcess(processID, bloomClock, vectorClock, eventCounter, processRefs)

      case ShowInternals =>
        context.log.info(s"dumping: Process $processID, latest VC: $vectorClock, latestBC: $bloomClock, latestExecution: $eventCounter")
        Behaviors.same


    }


  }

  def executingProcess(processID: Int,
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
          val newBC = performBCRules(bloomClock, processID, eventCounter, SEND_EVENT)
          if (gsnValue.gsn % numProcesses == 0)
            context.log.info(s"Sending Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
          whom ! RecvMessage(newVC, newBC)
          writingActor ! WriteToFile(s"${gsnValue.gsn}; $processID; $newEventCounter; ${newVC.mkString("[", ", ", "]")}; ${newBC.mkString("[", ", ", "]")}; SEND\n")
          idleProcess(processID, newBC, newVC, newEventCounter, processRefs)
        } else {
          idleProcess(processID, bloomClock, vectorClock, eventCounter, processRefs)
        }


      case ReceivedReceiveGSN(gsnValue, receivedVectorClock, receivedBloomClock) =>
        context.log.debug(s"Received GSN for receive event of ${gsnValue.gsn}")
        if (gsnValue.gsn != -1) {
          val newEventCounter = eventCounter + 1
          context.log.debug(s"Incrementing process Event (receive) counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, RULE_2, receivedVectorClock)
          val newBC = performBCRules(bloomClock, processID, eventCounter, RECV_EVENT, receivedBloomClock)
          if (gsnValue.gsn % numProcesses == 0)
            context.log.info(s"recv Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
          writingActor ! WriteToFile(s"${gsnValue.gsn}; $processID; $newEventCounter; ${newVC.mkString("[", ", ", "]")}; ${newBC.mkString("[", ", ", "]")}; RECV\n")
          idleProcess(processID, newBC, newVC, newEventCounter, processRefs)
        }
        else
          idleProcess(processID, bloomClock, vectorClock, eventCounter, processRefs)


      case ReceivedInternalGSN(gsnValue) =>
        context.log.debug(s"Received GSN for an Internal event of ${gsnValue.gsn}")
        if (gsnValue.gsn != -1) {
          val newEventCounter = eventCounter + 1
          context.log.debug(s"Incrementing Process Event (Internal) counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, RULE_1)
          val newBC = performBCRules(bloomClock, processID, eventCounter, INTERNAL_EVENT)
          if (gsnValue.gsn % numProcesses == 0)
            context.log.info(s"Internal Event Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
          writingActor ! WriteToFile(s"${gsnValue.gsn}; $processID; $newEventCounter; ${newVC.mkString("[", ", ", "]")}; ${newBC.mkString("[", ", ", "]")}; INTR\n")
          idleProcess(processID, newBC, newVC, newEventCounter, processRefs)
        } else {
          idleProcess(processID, bloomClock, vectorClock, eventCounter, processRefs)
        }


    }


  }

  // Utility functions
  def randomExec(probs: Map[Double, String]): String = {
    val rand = Random.nextDouble()
    val ranges = probs.tail.scanLeft(probs.head) {
      case ((prob1, _), (prob2, value)) => (prob1 + prob2, value)
    }
    val res = ranges.dropWhile(_._1 < rand).map(_._2).headOption.get
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
                     i: Int,
                     x: Int,
                     ruleID: Int,
                     bloomClockReceived: List[Int] = List.empty): List[Int] = {
    val m = M
    val hashSeeds = List(SEED_1, SEED_2, SEED_3, SEED_4)
    val hashing_tuple = (i, x)
    val hashValues = (1 to K).map(k => mmh3.productHash(hashing_tuple, hashSeeds(k - 1))).toList
    val bloomCounters = hashValues.map(hashValue => (hashValue & 0x7fffffff) % m)
    context.log.debug(s"${hashValues.length} + ${bloomCounters.toString()}")
    ruleID match {

      case INTERNAL_EVENT =>
        val newBloomClock = bloomClock.toArray
        bloomCounters.foreach(i => newBloomClock(i) += 1)
        context.log.debug(s"INT Before: ${bloomClock}, ${bloomClockReceived} After: ${newBloomClock.mkString(",")}, ${hashValues}, ${bloomCounters}")
        newBloomClock.toList

      case SEND_EVENT =>
        val newBloomClock = bloomClock.toArray
        bloomCounters.foreach(i => newBloomClock(i) += 1)
        context.log.debug(s"SEND Before: ${bloomClock}, ${bloomClockReceived} After: ${newBloomClock.mkString(",")}, ${hashValues}, ${bloomCounters}")
        newBloomClock.toList

      case RECV_EVENT =>
        val newBloomClock: Array[Int] = bloomClock.zip(bloomClockReceived).map(x => x._1.max(x._2)).toArray
        bloomCounters.foreach(i => newBloomClock(i) += 1)
        context.log.debug(s"RECV Before: ${bloomClock}, ${bloomClockReceived} After: ${newBloomClock.mkString(",")}, ${hashValues}, ${bloomCounters}")
        newBloomClock.toList
    }

  }


}
