package com.cs553.bloom

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout

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
  // Messages Process responds to
  final case class SendMessage(whom: ActorRef[ProcessMessages], processID: Int) extends ProcessMessages
  case class SendResponse(gsn: GsnValue) extends ProcessMessages
  case class RecvResponse(gsn: GsnValue) extends ProcessMessages

  // TODO
  final case class InitProcess() extends ProcessMessages
  final case class RecvMessage(vectorClock: List[Int], processID: Int) extends ProcessMessages
  final case object InternalEvent extends ProcessMessages
  final case object ShowInternals extends ProcessMessages
  private case class ReceivedSendGSN(gsnValue: GsnValue, whom: ActorRef[ProcessMessages]) extends ProcessMessages
  private case class ReceivedReceiveGSN(gsnValue: GsnValue, senderVectorClock: List[Int]) extends ProcessMessages



  def performVCRules(vectorClock: List[Int], processID: Int, ruleID: Int, receiptVectorClock: List[Int] = List.empty): List[Int] =
    ruleID match {
      // Rule 1
      case 1 => {
        // on sending message
        val newVC = vectorClock.updated(processID, vectorClock(processID) + 1)
        newVC
      }

      // Rule 2
      case 2 => {
        // on receival of message
        // get the max of both the VCs
        val maxVC = vectorClock.zip(receiptVectorClock).map(x => x._1.max(x._2))
        // execute R1
        val newVC = maxVC.updated(processID, maxVC(processID) + 1)
        newVC
      }
    }

  def performBCRules(bloomClock: List[Int], processID: Int, ruleID: Int): Nothing = ???

  def apply(numProcesses: Int, processID: Int, guardRef: ActorRef[GuardActor.Command]): Behavior[ProcessMessages] = {
    val bloomClock: List[Int] = List.fill(numProcesses)(0)
    val vectorClock: List[Int] = List.fill(numProcesses)(0)
    idleProcess(guardRef, processID, bloomClock, vectorClock, 0)

  }

  def idleProcess(guardActor: ActorRef[GuardActor.Command],
                  processID: Int,
                  bloomClock: List[Int],
                  vectorClock: List[Int],
                  eventCounter: Int): Behavior[ProcessMessages] = {
    Behaviors.receive { (context, message) =>
      context.log.info(s"Process Started: $processID, VC: ${vectorClock.toString}")
      message match {
        case SendMessage(whom, processID) => {
          implicit val timeout: Timeout = Timeout(1.seconds)
          val future = context.ask(guardActor, GuardActor.IncrementAndReplySender) {
            case Success(SendResponse(message)) => ReceivedSendGSN(message, whom)
            case Failure(_) => ReceivedSendGSN(GsnValue(-1), whom)
          }
          Behaviors.same
        }
        case ReceivedSendGSN(gsnValue, whom) => {
          context.log.info(s"Received GSN of ${gsnValue.gsn}")
          val newEventCounter = eventCounter + 1
          context.log.info(s"Incrementing Event counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, 1, List.empty)
          context.log.info(s"Process got a new VC: $processID, VC: ${newVC.toString}")
          whom ! RecvMessage(newVC, processID)
          idleProcess(guardActor, processID, bloomClock, newVC, newEventCounter)
        }

        case RecvMessage(senderVectorClock, senderId) => {
          implicit val timeout: Timeout = Timeout(1.seconds)
          val future: Unit = context.ask(guardActor, GuardActor.IncrementAndReplyReceiver) {
            case Success(RecvResponse(message)) => ReceivedReceiveGSN(message, senderVectorClock)
            case Failure(_) => ReceivedReceiveGSN(GsnValue(-1), senderVectorClock)
          }
          Behaviors.same
        }
        case ReceivedReceiveGSN(gsnValue, receivedVectorClock) => {
          context.log.info(s"Received GSN of ${gsnValue.gsn}")
          val newEventCounter = eventCounter + 1
          context.log.info(s"Incrementing Event counter to $newEventCounter, GSN: $gsnValue")
          val newVC = performVCRules(vectorClock, processID, 2, receivedVectorClock)
          context.log.info(s"Process got a new VC: $processID, VC: ${newVC.toString}")
          idleProcess(guardActor, processID, bloomClock, newVC, newEventCounter)
        }

        case ShowInternals => {
          context.log.info(s"dumping: Process $processID, latest VC: $vectorClock, latestExecution: $eventCounter")
          Behaviors.same
        }

        case InitProcess() => {
          Behaviors.unhandled
        }
        case InternalEvent => {
          Behaviors.unhandled
        }
      }
    }

  }





  // Messages Process is capable of sending


}
