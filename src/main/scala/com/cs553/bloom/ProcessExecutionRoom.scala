//package com.cs553.bloom
//
//import java.net.URLEncoder
//import java.nio.charset.StandardCharsets
//
//import akka.actor.typed.scaladsl.Behaviors
//import akka.actor.typed.{ActorRef, Behavior}
//
///*
//*
//* Created by: prajw
//* Date: 10-Nov-20
//*
//*/
//object ProcessExecutionRoom {
//
//  sealed trait MainCommand
//  case class GetSession(sessionName: String, replyTo: ActorRef[SessionEvent]) extends MainCommand
//  private final case class PublishSessionMessage(id: Int, name: String, msg: String)  extends MainCommand
//
//  sealed trait SessionEvent
//  // Send message / event
//  final case class SessionGranted(handle: ActorRef[PostMessage]) extends SessionEvent
//  final case class SessionDenied(reason: String) extends SessionEvent
//  // ACK that message is sent. Gets back the GSN TODO
//  final case class MessagePosted(screenName: String, message: String) extends SessionEvent
//
//  trait SessionCommand
//  final case class PostMessage(message: String) extends SessionCommand
//  private final case class NotifyClient(message: MessagePosted) extends SessionCommand
//
//  def executeProcesses(processes: List[ActorRef[SessionCommand]]): Behavior[MainCommand] = {
//
//    Behaviors.receive{(context, message) =>
//      message match {
//
//        case GetSession(processName, processRef) =>
//          // create a child process
//          val processSes = context.spawn(
//            processSession(context.self, processName, processRef),
//            name = URLEncoder.encode(processName, StandardCharsets.UTF_8.name))
//          processRef ! SessionGranted(processSes)
//          executeProcesses(processSes :: processes)
//
//
//        case PublishSessionMessage(id, name, msg) =>
//          val notification = NotifyClient(MessagePosted(name, msg))
//          processes.foreach(_ ! notification)
//          Behaviors.same
//      }
//
//    }
//
//  }
//
//
//  def processSession(guardActor: ActorRef[MainCommand],
//                     processName: String,
//                     processRef: ActorRef[SessionEvent]): Behavior[SessionCommand] = {
//    Behaviors.receiveMessage {
//
//      case PostMessage(message) =>
//        // from client, publish to others via Guard
//        guardActor ! PublishSessionMessage(1, processName, message)
//        Behaviors.same
//      case NotifyClient(message) =>
//        // published message from the guard
//        processRef ! message
//        Behaviors.same
//    }
//
//  }
//
//
//  def apply(): Behavior[MainCommand] =
//    executeProcesses(List.empty)
//
//
//
//
//}
