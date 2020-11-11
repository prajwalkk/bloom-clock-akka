//package com.cs553.bloom
//
//import akka.actor.typed.Behavior
//import akka.actor.typed.scaladsl.Behaviors
//
//
///*
//*
//* Created by: prajw
//* Date: 09-Nov-20
//*
//*/
//object SimulationActor {
//
//  import ProcessExecutionRoom._
//
//  def apply(): Behavior[SessionEvent] = {
//    Behaviors.setup{ context =>
//      Behaviors.receiveMessage {
//        case SessionGranted(handle) =>
//          handle ! PostMessage("hello World")
//          Behaviors.same
//        case MessagePosted(screenName, message) =>
//          context.log.info("message sent by '{}': {}", screenName, message)
//          Behaviors.same
//      }
//
//    }
//  }
//}
