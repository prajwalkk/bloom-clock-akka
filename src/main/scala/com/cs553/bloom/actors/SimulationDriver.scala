package com.cs553.bloom.actors

import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import SimMain.Start
import com.cs553.bloom.utils.ApplicationConstants
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

/*
*
* Created by: prajw
* Date: 13-Nov-20
*
*/
object SimulationDriver extends LazyLogging {

  val config: Config = ConfigFactory.load("constants.conf")
  val fileName: String = config.getString("FileName")
  val arrN: List[Int] = config.getIntList("numProcesses").asScala.map(i => i.toInt).toList
  val arrK: List[Int] = config.getIntList("k").asScala.map(i => i.toInt).toList
  val arrMRatio: List[Double] = config.getDoubleList("m").asScala.map(i => i.toDouble).toList

  def main(args: Array[String]): Unit = {

    for {
      n <- arrN
      k <- arrK
      mRat <- arrMRatio
    } yield{

      Thread.sleep(10000)

      val system = ActorSystem(Main(n, k, (mRat * n).toInt, fileName), "applicaion")
      logger.info(s"===================================starting:(N, K, M) = ${(n, k, (mRat * n).toInt)} ==================")
      // Execute for N + 35 seconds. Taken 60 as a buffer
      // Run for 10 minutes
      Thread.sleep((3 * 60 * 1000) + (60 * 1000))
      logger.info("====================================terminating========================================================")
      system.terminate()
    }

  }

  object Main {
    def apply(n: Int, k: Int, m: Int, fileName: String): Behavior[NotUsed] = Behaviors.setup { context =>
      val simDriver = context.spawn(SimMain(n, k, m, fileName), "SimDriver")
      simDriver ! Start
      Behaviors.empty
    }
  }


}