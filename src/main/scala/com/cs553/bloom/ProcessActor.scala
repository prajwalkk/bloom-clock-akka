package com.cs553.bloom

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import com.cs553.bloom.ApplcationConstants._
import scala.util.hashing.{MurmurHash3 => mmh3}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/*
*
* Created by: prajw
* Date: 09-Nov-20
*
*/
object ProcessActor {

  sealed trait ProcessMessages
  // Messages Process responds to That comes from a Process Actor (can be self)
  case class SendResponse(gsn: GsnValue) extends ProcessMessages
  case class RecvResponse(gsn: GsnValue) extends ProcessMessages
  case class InternalResponse(gsn: GsnValue) extends ProcessMessages

  // TODO
  final case class InitProcess() extends ProcessMessages

  // Main Messages that define the Actor's jobs. These messages come from the simulator
  final case class SendMessage(whom: ActorRef[ProcessMessages]) extends ProcessMessages
  final case class RecvMessage(vectorClock: List[Int], bloomClock: List[Int]) extends ProcessMessages
  final case object InternalEvent extends ProcessMessages
  final case object ShowInternals extends ProcessMessages

  // Adapted Responses. Process get the message from the guard and Marshals and sends back to itself to the future
  // These are blocking. Meaning, a process wont execute any event until it receives these responses
  private case class ReceivedSendGSN(gsnValue: GsnValue, whom: ActorRef[ProcessMessages]) extends ProcessMessages
  private case class ReceivedReceiveGSN(gsnValue: GsnValue, senderVectorClock: List[Int], senderBloomClock: List[Int]) extends ProcessMessages
  private case class ReceivedInternalGSN(gsnValue: GsnValue) extends ProcessMessages



  def performVCRules(vectorClock: List[Int],
                     processID: Int,
                     ruleID: Int,
                     receiptVectorClock: List[Int] = List.empty): List[Int] =
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

    def performBCRules(bloomClock: List[Int],
                       i: Int, x: Int,
                       ruleID: Int,
                       bloomClockReceived:List[Int] = List.empty): List[Int] = {
      val m = bloomClock.length
      val hashSeeds = List(SEED_1, SEED_2, SEED_3, SEED_4)
      val hashing_tuple = (i,x)
      val hashValues = (1 to K).map(k => mmh3.productHash(hashing_tuple, hashSeeds(k))).toList
      val bloomCounters = hashValues.map(hashValue => Math.abs(hashValue % m))
      ruleID match {
        case INTERNAL_EVENT =>
          val newBloomClock = bloomClock.toArray
          bloomCounters.foreach(i => newBloomClock(i)+=1)
          newBloomClock.toList
        case SEND_EVENT =>
          val newBloomClock = bloomClock.toArray
          bloomCounters.foreach(i => newBloomClock(i)+=1)
          newBloomClock.toList
        case RECV_EVENT =>
          val newBloomClock: Array[Int] = bloomClock.zip(bloomClockReceived).map(x => x._1.max(x._2)).toArray
          bloomCounters.foreach(i => newBloomClock(i)+=1)
          newBloomClock.toList
      }
    }

  def apply(numProcesses: Int, processID: Int,
            guardRef: ActorRef[GuardActor.Command]): Behavior[ProcessMessages] = {
    val bloomClock: List[Int] = List.fill((numProcesses * BLOOM_CLOCK_LENGTH_RATIO).toInt)(0)
    val vectorClock: List[Int] = List.fill(numProcesses)(0)
    idleProcess(guardRef, processID, bloomClock, vectorClock, 0)

  }

  def idleProcess(guardActor: ActorRef[GuardActor.Command],
                  processID: Int,
                  bloomClock: List[Int],
                  vectorClock: List[Int],
                  eventCounter: Int): Behavior[ProcessMessages] = {
    Behaviors.receive { (context, message) =>
      context.log.debug(s"Process Started: $processID, VC: ${vectorClock.toString}")
      message match {


        case SendMessage(whom) =>
          implicit val timeout: Timeout = Timeout(1.seconds)
          context.ask(guardActor, GuardActor.IncrementAndReplySender) {
            case Success(SendResponse(message)) => ReceivedSendGSN(message, whom)
            case Failure(_) => ReceivedSendGSN(GsnValue(-1), whom)
          }
          Behaviors.same

        case ReceivedSendGSN(gsnValue, whom) =>
          context.log.debug(s"Received GSN for a send event of ${gsnValue.gsn}")
          if (gsnValue.gsn != -1) {
            val newEventCounter = eventCounter + 1
            context.log.debug(s"Incrementing Process Event (send) counter to $newEventCounter, GSN: $gsnValue")
            val newVC = performVCRules(vectorClock, processID, RULE_1)
            val newBC = performBCRules(bloomClock, processID, newEventCounter, SEND_EVENT)
            context.log.info(s"Sending Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
            whom ! RecvMessage(newVC, newBC)
            idleProcess(guardActor, processID, newBC, newVC, newEventCounter)
          } else {
            Behaviors.same
          }

        case RecvMessage(senderVectorClock, senderBloomCLock) =>
          implicit val timeout: Timeout = Timeout(1.seconds)
          context.ask(guardActor, GuardActor.IncrementAndReplyReceiver) {
            case Success(RecvResponse(message)) => ReceivedReceiveGSN(message, senderVectorClock, senderBloomCLock)
            case Failure(_) => ReceivedReceiveGSN(GsnValue(-1), senderVectorClock, senderBloomCLock)
          }
          Behaviors.same

        case ReceivedReceiveGSN(gsnValue, receivedVectorClock, receivedBloomClock) =>
          context.log.debug(s"Received GSN for receive event of ${gsnValue.gsn}")
          if (gsnValue.gsn != -1) {
            val newEventCounter = eventCounter + 1
            context.log.debug(s"Incrementing process Event (receive) counter to $newEventCounter, GSN: $gsnValue")
            val newVC = performVCRules(vectorClock, processID, RULE_2, receivedVectorClock)
            val newBC = performBCRules(bloomClock, processID, newEventCounter, RECV_EVENT, receivedBloomClock)
            context.log.info(s"recv Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
            idleProcess(guardActor, processID, newBC, newVC, newEventCounter)
          }
          else
            Behaviors.same

        case ShowInternals =>
          context.log.info(s"dumping: Process $processID, latest VC: $vectorClock, latestBC: $bloomClock, latestExecution: $eventCounter")
          Behaviors.same

        case InitProcess() =>
          Behaviors.unhandled

        case InternalEvent =>
          implicit val timeout: Timeout = Timeout(1.seconds)
          context.ask(guardActor, GuardActor.IncrementAndReplySender) {
            case Success(InternalResponse(message)) => ReceivedInternalGSN(message)
            case Failure(_) => ReceivedInternalGSN(GsnValue(-1))
          }
          Behaviors.same

        case ReceivedInternalGSN(gsnValue) =>
          context.log.debug(s"Received GSN for an Internal event of ${gsnValue.gsn}")
          if (gsnValue.gsn != -1) {
            val newEventCounter = eventCounter + 1
            context.log.debug(s"Incrementing Process Event (Internal) counter to $newEventCounter, GSN: $gsnValue")
            val newVC = performVCRules(vectorClock, processID, RULE_1)
            val newBC = performBCRules(bloomClock, processID, newEventCounter, INTERNAL_EVENT)
            context.log.info(s"Internal Event Process: $processID got a new VC, VC: ${newVC.toString}, BC: ${newBC.toString}")
            idleProcess(guardActor, processID, newBC, newVC, newEventCounter)
          } else {
            Behaviors.same
          }


      }
    }


  }
}
