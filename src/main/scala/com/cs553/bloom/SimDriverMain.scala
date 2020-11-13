package com.cs553.bloom

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import com.cs553.bloom.ProcessActor.{SendMessage, ShowInternals}


/*
*
* Created by: prajw
* Date: 09-Nov-20
*
*/
object SimDriverMain extends App  {

  object Main {
    def apply(n: Int): Behavior[NotUsed] = {

      Behaviors.setup { context =>
        val guardActor = context.spawn(GuardActor(4), "guard")
        val processActorsRef = (0 until n).map(i => context.spawn(ProcessActor(n, i, guardActor), s"process_$i"))
        // TODO Debug
        processActorsRef(0) ! SendMessage(processActorsRef(1))
        processActorsRef.foreach(act => act ! ShowInternals)
        processActorsRef(0) ! SendMessage(processActorsRef(1))
        processActorsRef.foreach(act => act ! ShowInternals)
        processActorsRef(1) ! SendMessage(processActorsRef(0))
        Thread.sleep(15000)
        context.log.info("=========================================================================")
        processActorsRef(0) ! ShowInternals
        processActorsRef(1) ! ShowInternals
        Behaviors.stopped
      }
    }


  }


  ActorSystem(Main(20), "Demo")


}

//
