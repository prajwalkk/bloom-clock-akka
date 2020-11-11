//package com.cs553.bloom
//
//import akka.NotUsed
//import akka.actor.typed.scaladsl.Behaviors
//import akka.actor.typed.{ActorRef, Behavior}
//import com.cs553.bloom.ProcessExecutionRoom.SessionEvent
//
///*
//*
//* Created by: prajw
//* Date: 10-Nov-20
//*
////*/
////object ProcessWatchers {
////
////  def apply(processRef: List[ActorRef[SessionEvent]]): Behavior[NotUsed] = {
////    watchActors(processRef)
////  }
////
////  def watchActors(processRef: List[ActorRef[SessionEvent]]): Behavior[NotUsed] = {
////    val num_watched = processRef.length
////    Behaviors.setup { context =>
////        processRef.foreach(p => context)
////      }
////    Behaviors.receive{
////
////    }
////  }
//
//
//}
