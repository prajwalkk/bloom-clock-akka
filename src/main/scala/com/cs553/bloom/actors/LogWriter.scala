package com.cs553.bloom.actors

import java.nio.file.{OpenOption, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Formatter.DateTime

import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import com.cs553.bloom.utils.ApplicationConstants

import scala.concurrent.Future

/*
*
* Created by: prajw
* Date: 20-Nov-20
*
*/
object LogWriter {

  def apply(): Behavior[WriterMessages] = {
    Behaviors.setup { context =>
      new LogWriter(context)
        .fileWriter(ApplicationConstants.fileName +
          "_" +
          ApplicationConstants.N +
          "_" + ApplicationConstants.K +
          "_" + ApplicationConstants.BLOOM_CLOCK_LENGTH_RATIO +
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss")) +
          ".csv")
    }
  }

  sealed trait WriterMessages

  case class InitFile(file: String) extends WriterMessages

  case class WriteToFile(txt: String) extends WriterMessages

}

class LogWriter(context: ActorContext[LogWriter.WriterMessages]) {

  import LogWriter._

  val fileOpenOptions: Set[OpenOption] = Set(StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)

  def fileWriter(fileName: String): Behavior[WriterMessages] = {
    Behaviors.receiveMessage {
      case WriteToFile(txt) =>
        implicit val system: ActorSystem[Nothing] = context.system
        val stringSource: Source[String, NotUsed] = Source.single(txt)
        val fileResult: Future[IOResult] = stringSource.map(char => ByteString(char))
          .runWith(FileIO.toPath(Paths.get(fileName), fileOpenOptions))
        Behaviors.same
    }
  }
}

