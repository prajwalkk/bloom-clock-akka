package com.cs553.bloom

import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.cs553.bloom.SimMain.Start
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/*
*
* Created by: prajw
* Date: 13-Nov-20
*
*/
object SimulationDriver extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem(Main(ApplicationConstants.N), "Demo")
    Thread.sleep(130000)
    logger.info("terminating")
    system.terminate()
  }

  object Main {
    def apply(n: Int): Behavior[NotUsed] = Behaviors.setup { context =>
      val simDriver = context.spawn(SimMain(n), "SimDriver")
      simDriver ! Start
      Behaviors.empty
    }
  }


}