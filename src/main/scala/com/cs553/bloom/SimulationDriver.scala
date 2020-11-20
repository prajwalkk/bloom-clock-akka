package com.cs553.bloom

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.actor.typed.internal.adapter.ActorRefFactoryAdapter
import akka.actor.typed.scaladsl.Behaviors
import com.cs553.bloom.ProcessActor.{ExecuteSomething, ShowInternals}
import com.cs553.bloom.SimMain.Start

import scala.concurrent.duration.DurationInt

/*
*
* Created by: prajw
* Date: 13-Nov-20
*
*/
object SimulationDriver {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem(Main(2), "Demo")
  }

  object Main {
    def apply(n: Int): Behavior[NotUsed] = Behaviors.setup { context =>
      val simDriver = context.spawn(SimMain(5), "SimDriver")
      simDriver ! Start
//      val guardActor = context.spawn(GuardActor(4), "guard")
//      val processActorsRef = (0 until n).map(i => context.spawn(ProcessActor(n, i, guardActor), s"process_$i"))
//      processActorsRef.foreach(p => p ! ProcessActor.InitProcess)
//      // TODO Debug
//      Thread.sleep(5000)
//      processActorsRef(0) ! ExecuteSomething
//      processActorsRef.foreach(act => act ! ShowInternals)
//      processActorsRef(0) ! ExecuteSomething
//      processActorsRef.foreach(act => act ! ShowInternals)
//      processActorsRef(1) ! ExecuteSomething
      Behaviors.empty
    }
  }


}