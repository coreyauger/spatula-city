package io.surfkit.spatulacity.tasks

import java.io.File
import java.nio.file._
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat


object StatsTasks {
  import scala.collection.JavaConversions._
  import com.github.nscala_time.time.OrderingImplicits._

  val dirDateFormat = DateTimeFormat.forPattern("yyyyMMdd")

  sealed trait PriceTs{
    def date: DateTime
    def volume: Double
    def open: Double
    def high: Double
    def low: Double
    def close: Double
  }
  case class KibotPrice(date: DateTime, volume: Double, open: Double, high: Double, low: Double, close: Double) extends PriceTs
  case class QuantquotePrice(date: DateTime, volume: Double, open: Double, high: Double, low: Double, close: Double) extends PriceTs

  val qDateFormatter = DateTimeFormat.forPattern("yyyyMMdd HHmm")
  val kDateFormatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm")

  def generateStats(quantquote: java.io.File, kibot: java.io.File, outputDir: java.io.File)(implicit system: ActorSystem, materializer: ActorMaterializer) = {

    try {
      val counter = new AtomicInteger(0)
      val qStocks = quantquote.listFiles.map(x => x.getName -> x).toMap
      val kStocks = kibot.listFiles.map(x => x.getName -> x).toMap

      val allStocks = qStocks.keySet ++ kStocks.keySet
      allStocks.foreach{ stock =>
        println(s"\nProcessing Stats for stock: ${stock}")
        val qprices = qStocks.get(stock).map(f => Files.readAllLines(Paths.get(f.getAbsolutePath) ).toList ).getOrElse(List.empty[String]).map{ l =>
          val split = l.split(",")
          val date =
            if(split(1).size == 3)qDateFormatter.withZone(DateTimeZone.forID("America/New_York")).parseDateTime(s"${split(0)} 0${split(1)}")
            else qDateFormatter.withZone(DateTimeZone.forID("America/New_York")).parseDateTime(s"${split(0)} ${split(1)}")
          // open, high, low, close, vol ?
          QuantquotePrice(date = date, volume = split(6).toDouble, open = split(2).toDouble, high = split(3).toDouble, low = split(4).toDouble, close = split(5).toDouble)
        }
        val kprices = kStocks.get(stock).map(f => Files.readAllLines(Paths.get(f.getAbsolutePath) ).toList ).getOrElse(List.empty[String]).map{ l =>
          val split = l.split(",")
          val date = kDateFormatter.withZone(DateTimeZone.forID("America/New_York")).parseDateTime(s"${split(0)} ${split(1)}")
          KibotPrice(date = date, volume = split(6).toDouble, open = split(2).toDouble, high = split(3).toDouble, low = split(4).toDouble, close = split(5).toDouble)
        }
        (qprices ++ kprices).groupBy(_.date).toList.sortBy(_._1).foreach{
          case (time, xs) =>
            (xs.find(_.isInstanceOf[QuantquotePrice]), xs.find(_.isInstanceOf[KibotPrice])) match{
              case (Some(q:QuantquotePrice), Some(k:KibotPrice)) => print(".")
              case (None, Some(k:KibotPrice)) => print("k")
              case (Some(q:QuantquotePrice), None) => print("q")
              case (None, None) =>
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
