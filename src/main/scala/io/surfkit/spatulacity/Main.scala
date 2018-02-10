package io.surfkit.spatulacity

import java.io.File
import akka.actor.ActorSystem
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import io.surfkit.spatulacity.tasks._

object Main extends App {

  implicit val system: ActorSystem = ActorSystem()
  val decider: Supervision.Decider = {
    case _ => Supervision.Resume
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  //val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm")

  @inline def defined(line: String) = {
    line != null && line.nonEmpty
  }
  Iterator.continually(scala.io.StdIn.readLine).takeWhile(defined(_)).foreach{line =>
    println("read " + line)
    CommandParser.parse(line.split(' ')).map { cmd =>
      cmd.mode match {
        case CommandParser.Mode.transform =>
          cmd.transform match{
            case "sanity" =>
              TransformTasks.toSaneInputFile(cmd.file, cmd.out, if(cmd.symbol == "")None else Some(cmd.symbol) )
            case x =>
              println(s"Unknown transform: ${x}")
          }

        case x => println(s"Unknown command '${x}'.")
      }
    }
  }

}

