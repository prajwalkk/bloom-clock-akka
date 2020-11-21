package com.cs553.bloom.actors

import java.nio.file.{FileSystems, OpenOption, Path, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Formatter.DateTime

import akka.stream.alpakka.file.scaladsl.Directory
import akka.NotUsed
import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.stream.IOResult
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.{FileIO, Sink, Source}
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

  def apply(N: Int, K: Int, M: Int, fileName: String): Behavior[WriterMessages] = {
    Behaviors.setup { context =>
      new LogWriter(context)
        .fileWriter(fileName +
          "_" +
          N +
          "_" + K +
          "_" + M +
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss")) +
          ".csv")
    }
  }

  sealed trait WriterMessages

  case object InitFile extends WriterMessages

  case class WriteToFile(txt: String) extends WriterMessages

}

class LogWriter(context: ActorContext[LogWriter.WriterMessages]) {

  import LogWriter._

  val fileOpenOptions: Set[OpenOption] = Set(StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)

  val currDate: String = LocalDateTime.now().toLocalDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  def fileWriter(fileName: String): Behavior[WriterMessages] = {
    Behaviors.receiveMessage {

      case InitFile =>
        implicit val system: ActorSystem[Nothing] = context.system
        val fs = FileSystems.getDefault
        val dir = fs.getPath("sims")
        val flow: Flow[Path,Path, NotUsed] = Directory.mkdirs()
        val created: Future[Seq[Path]] = Source(Seq(dir.resolve(currDate + "/" + ApplicationConstants.RUN_NAME))).via(flow).runWith(Sink.seq)
        Behaviors.same

      case WriteToFile(txt) =>
        implicit val system: ActorSystem[Nothing] = context.system
        val stringSource: Source[String, NotUsed] = Source.single(txt)
        val fullFileName = s"sims/${currDate}/${ApplicationConstants.RUN_NAME}/" + fileName
        val fileResult: Future[IOResult] = stringSource.map(char => ByteString(char))
          .runWith(FileIO.toPath(Paths.get(fullFileName), fileOpenOptions))
        Behaviors.same
    }
  }
}

