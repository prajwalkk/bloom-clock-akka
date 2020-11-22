package com.cs553.bloom.actors

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import SimMain.MainCommand
import akka.actor.PoisonPill

import scala.concurrent.duration.{DurationInt, FiniteDuration}


/*
*
* Created by: prajw
* Date: 09-Nov-20
*
*/

object SimMain {

  def apply(n: Int, k: Int, m: Int, fileName: String): Behavior[MainCommand] = {

    Behaviors.setup { ctx =>
      val guardActor = ctx.spawn(GuardActor(), "guard")
      val writerActor = ctx.spawn(LogWriter(n, k, m, fileName), "writer")
      writerActor ! LogWriter.InitFile
      Thread.sleep(5000)
      writerActor ! LogWriter.WriteToFile("GSN; i; x; VC; BC; TYPE\n")
      val processActorsRef = (0 until n).map(i => ctx.spawn(ProcessActor(n, k, m, i, guardActor, writerActor), s"process_$i"))
      Thread.sleep(15000)
      processActorsRef.foreach(p => p ! ProcessActor.InitProcess)
      Thread.sleep(15000)
      new SimMain(ctx, n, guardActor, processActorsRef.toList).idle(5.millis ,0)
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

  private def idle(duration: FiniteDuration, count: Int): Behavior[MainCommand] = {
    Behaviors.withTimers[MainCommand] { timers =>
      timers.startSingleTimer(Begin, TimeOut, duration)
      ctx.log.debug("Begin wait done")
      ctx.log.debug(count.toString)
      if(count > (n + 11)) {
        Thread.sleep(120 * 1000)
        Behaviors.stopped
      }else{
        active(count)
      }
    }
  }

  private def active(count: Int): Behavior[MainCommand] = {
    import ProcessActor._
    Behaviors.receiveMessage[MainCommand] {
      case Start | TimeOut =>
        ctx.log.debug("timeout came")
        processRef.foreach(p => p ! ExecuteSomething)
        //processRef.foreach(act => act ! ShowInternals)
        idle(60.millis, count + 1)
    }
  }

}



