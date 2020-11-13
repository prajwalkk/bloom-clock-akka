package com.cs553.bloom

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

/*
*
* Created by: prajw
* Date: 09-Nov-20
*
*/
object GuardActor {
  import ProcessActor._
  sealed trait Command
  final case class IncrementAndReplySender(replyTo: ActorRef[ProcessActor.ProcessMessages]) extends Command
  final case class IncrementAndReplyReceiver(replyTo: ActorRef[ProcessActor.ProcessMessages]) extends Command
  final case class IncrementAndReplyInternal(replyTo: ActorRef[ProcessActor.ProcessMessages]) extends Command

  case object Increment extends Command


  def apply(maxVal: Int): Behavior[Command] =
    gsnCounter(0, maxVal)

  def getNewGsn(oldGsn:Int, maxVal: Int): Int = {
    val newGsn = oldGsn + 1
    if (newGsn > maxVal) {
      // TODO:
      0
    }
    else {
      // TODO: reply to the actor requesting GSN
      oldGsn + 1
    }
  }

  private def gsnCounter(gsn: Int, maxVal: Int): Behavior[Command] = {
    Behaviors.receive { (context, message) =>
      message match {

        case IncrementAndReplySender(replyTo) => {
          val newGsn = getNewGsn(gsn, 1000)
          replyTo ! SendResponse(GsnValue(newGsn))
          gsnCounter(newGsn, maxVal)
        }
        case IncrementAndReplyReceiver(replyTo) => {
          val newGsn = getNewGsn(gsn, 1000)
          replyTo ! RecvResponse(GsnValue(newGsn))
          gsnCounter(newGsn, maxVal)
        }

      }
    }
  }


}
