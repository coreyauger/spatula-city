package io.surfkit.spatulacity.tasks

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString
import java.io.File
import java.nio.file._

import scala.concurrent.duration._
import org.joda.time.{DateTime, DateTimeZone, Minutes, Seconds}
import org.joda.time.format.DateTimeFormat

import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}


object TransformTasks {

  val dirDateFormat = DateTimeFormat.forPattern("yyyyMMdd")

  case class Csv(symbol: String, file: File)
  case class Day(dir: File, date: DateTime, stocks: Map[String, Csv])

  def toSaneInputFile(root: java.io.File, outputDir: java.io.File, dropUntil: Option[String] = None)(implicit system: ActorSystem, materializer: ActorMaterializer) = {
    try {
      val counter = new AtomicInteger(0)
      val days = root.listFiles.filter(_.isDirectory).sortBy(_.getName).map{ dateDir =>
        println(dateDir.getAbsolutePath)
        val csvMap = dateDir.listFiles.map{ c =>
          val sym = c.getName.replace("table_", "").replace(".csv","")
          sym -> Csv(sym, c)
        }.toMap
        val date = dirDateFormat.parseDateTime(dateDir.getName.replace("allstocks_",""))
        Day(dateDir, date, csvMap)
      }
      val stocks =
        dropUntil match{
          case None => days.flatMap(_.stocks.values).toSet
          case Some(until) => days.flatMap(_.stocks.values).toSet.dropWhile(_.symbol.toUpperCase != until.toUpperCase )
        }

      println(s"the directory is: ${days}")
      println(s"stocks: ${stocks}")
      stocks.foreach{ stock =>
        val filePath = Paths.get(outputDir.getAbsolutePath + "/" + stock.symbol.toUpperCase)
        if (!Files.exists(filePath))Files.createFile(filePath)
        println(s"write: ${filePath}")
        days.foreach{ day =>
          day.stocks.get(stock.symbol).foreach { csv =>
            import scala.collection.JavaConversions._
            val lines = Files.readAllLines(Paths.get(csv.file.getAbsolutePath) )
            Files.write(filePath, lines.toList.mkString("", "\n","\n").getBytes(), StandardOpenOption.APPEND)
            if (counter.addAndGet(1) % 10 == 0) print(".")
          }
        }

      }

      Thread.currentThread.join()
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }


}
