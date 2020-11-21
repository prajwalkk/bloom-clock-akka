package com.cs553.bloom

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.cs553.bloom.SimMain.MainCommand

import scala.concurrent.duration.{DurationInt, FiniteDuration}


/*
*
* Created by: prajw
* Date: 09-Nov-20
*
*/

object SimMain {

  def apply(n: Int): Behavior[MainCommand] = {

    Behaviors.setup { ctx =>
      val guardActor = ctx.spawn(GuardActor(), "guard")
      val writerActor = ctx.spawn(LogWriter(), "writer")
      val processActorsRef = (0 until n).map(i => ctx.spawn(ProcessActor(n, i, guardActor, writerActor), s"process_$i"))
      Thread.sleep(15000)
      processActorsRef.foreach(p => p ! ProcessActor.InitProcess)
      new SimMain(ctx, n, guardActor, processActorsRef.toList).idle(5.millis)
    }
  }

  // Protocols
  sealed trait MainCommand

  final case class Stop(messages: String) extends MainCommand

  final case object Start extends MainCommand

  final case object Begin extends MainCommand

  final case object TimeOut extends MainCommand


}

class SimMain(ctx: ActorContext[MainCommand],
              n: Int,
              guardRef: ActorRef[GuardActor.Command],
              processRef: List[ActorRef[ProcessActor.ProcessMessages]]) {

  import SimMain._

  private def idle(duration: FiniteDuration): Behavior[MainCommand] = {
    Behaviors.withTimers[MainCommand] { timers =>
      timers.startSingleTimer(Begin, TimeOut, duration)
      ctx.log.debug("Begin wait done")
      active()
    }
  }

  private def active(): Behavior[MainCommand] = {
    import ProcessActor._
    Behaviors.receiveMessage[MainCommand] {
      case Start | TimeOut =>
        ctx.log.debug("timeout came")
        processRef.foreach(p => p ! ExecuteSomething)
        //processRef.foreach(act => act ! ShowInternals)
        idle(15.millis)
    }
  }

}



