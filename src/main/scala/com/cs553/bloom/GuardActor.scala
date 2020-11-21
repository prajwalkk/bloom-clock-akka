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

  def apply(): Behavior[Command] =
    gsnCounter(0)

  def getNewGsn(oldGsn: Int): Int = {
    oldGsn + 1
  }

  private def gsnCounter(gsn: Int): Behavior[Command] = {
    Behaviors.receiveMessage {

      case IncrementAndReplySender(replyTo) => {
        val newGsn = getNewGsn(gsn)
        replyTo ! SendResponse(GsnValue(newGsn))
        gsnCounter(newGsn)
      }
      case IncrementAndReplyReceiver(replyTo) => {
        val newGsn = getNewGsn(gsn)
        replyTo ! RecvResponse(GsnValue(newGsn))
        gsnCounter(newGsn)
      }

      case IncrementAndReplyInternal(replyTo) => {
        val newGsn = getNewGsn(gsn)
        replyTo ! InternalResponse(GsnValue(newGsn))
        gsnCounter(newGsn)
      }

    }
  }

  sealed trait Command

  final case class IncrementAndReplySender(replyTo: ActorRef[ProcessActor.ProcessMessages]) extends Command

  final case class IncrementAndReplyReceiver(replyTo: ActorRef[ProcessActor.ProcessMessages]) extends Command

  final case class IncrementAndReplyInternal(replyTo: ActorRef[ProcessActor.ProcessMessages]) extends Command

  case object Increment extends Command


}
