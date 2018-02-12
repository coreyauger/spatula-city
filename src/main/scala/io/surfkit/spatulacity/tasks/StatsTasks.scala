package io.surfkit.spatulacity.tasks

import java.io.File
import java.nio.file._
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.joda.time.{DateTime, DateTimeZone, Minutes}
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
        var lastPrice: PriceTs = null
        var bothCount = 0
        var negativeValuesCount = 0
        var fillCount = 0
        var kibotMissingCount = 0
        var quantquoteMissingCount = 0
        var precentOfPrice = 0.0
        val gapCounts = scala.collection.mutable.HashMap(
          5 -> 0,
          10 -> 0,
          15 -> 0,
          20 -> 0,
          30 -> 0,
          45 -> 0,
          60 -> 0,
          90 -> 0
        )
        val closeDifferences = scala.collection.mutable.HashMap(
          0.005 -> 0,
          0.01 -> 0,
          0.015 -> 0,
          0.02 -> 0,
          0.05 -> 0,
          0.1 -> 0
        )
        val daysToBailOn = scala.collection.mutable.HashSet.empty[DateTime]
        val master = scala.collection.mutable.ArrayBuffer.empty[PriceTs]

        (qprices ++ kprices).groupBy(_.date).toList.sortBy(_._1).foreach{
          case (time, xs) if time.isAfter(time.withTime(9, 29, 0, 0)) && time.isBefore(time.withTime(16, 1, 0, 0)) =>
            val fill = fillForward(lastPrice, time)
            gapCounts.keys.foreach{ k =>
              if(fill.size >=  k)gapCounts(k) = gapCounts(k) + 1
              if(fill.size >= 20){ // anything more then a 20 min gap is not acceptable for us
                daysToBailOn.add(time.withTime(0, 0, 0, 0))
              }
            }
            fillCount = fillCount + fill.size
            (xs.find(_.isInstanceOf[QuantquotePrice]), xs.find(_.isInstanceOf[KibotPrice])) match{
              case (Some(q:QuantquotePrice), Some(k:KibotPrice)) =>
                negativeValuesCount = negativeValuesCount + countNegative(k)
                bothCount = bothCount + 1
                val diff = Math.abs(q.close - k.close)
                precentOfPrice = 1.0 - (q.close / (diff + q.close))
                closeDifferences.keys.foreach{ k =>
                  if(precentOfPrice >=  k)closeDifferences(k) = closeDifferences(k) + 1
                }
                lastPrice = q
                master.addAll(fill)
                master.add(q)
              case (None, Some(k:KibotPrice)) =>
                negativeValuesCount = negativeValuesCount + countNegative(k)
                quantquoteMissingCount = quantquoteMissingCount + 1
                /*if(precentOfPrice < 0.005) {  // if we are in the same ballpark .. use this record to heal the hole
                  lastPrice = k
                  master.add(k)
                }*/
              case (Some(q:QuantquotePrice), None) =>
                kibotMissingCount = kibotMissingCount + 1
                lastPrice = q
                master.addAll(fill)
                master.add(q)
              case _ => throw new Exception("WTF yo!")
            }
          case _ => // not in market hours so forget it !
        }
        println(s"Minutes: ${bothCount}")
        println(s"Quantquote Missing Minutes: ${quantquoteMissingCount}")
        println(s"Kibot Missing Minutes: ${kibotMissingCount}")
        println(s"Missing Minutes: ${fillCount}")
        println(s"Negative Values: ${negativeValuesCount}")
        gapCounts.keys.toList.sorted.foreach{ k =>
          println(s"Gaps ${k}: ${gapCounts(k)}")
        }
        closeDifferences.keys.toList.sorted.foreach{ k =>
          println(s"Errors ${k}: ${closeDifferences(k)}")
        }
        println(s"writing MASTER: ${stock}")
        val masterLines = master.filterNot{ x =>
          daysToBailOn.contains(x.date.withTime(0,0,0,0))
        }.map{ x =>
          s"${x.date},${x.open},${x.high},${x.low},${x.volume}"
        }
        if(!masterLines.isEmpty)
          Files.write(Paths.get(outputDir.getAbsolutePath + "/" + stock.toUpperCase), masterLines.toList.mkString("", "\n","\n").getBytes(), StandardOpenOption.CREATE)
        println("\n")

      }
      Thread.currentThread.join()
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
  }

  def countNegative(k: KibotPrice) =
    if(List(k.open, k.close, k.high, k.low).forall(_ >= 0.0) ) 0 else 1

  def fillForward(lastPrice: PriceTs, time: DateTime): Seq[PriceTs] = {
    if(lastPrice != null && lastPrice.date.getDayOfYear == time.getDayOfYear) {
      val toFill = Math.abs(Minutes.minutesBetween(lastPrice.date, time).getMinutes)
      (1 to toFill-1).map{ i =>
        lastPrice match{
          case q: QuantquotePrice => q.copy(date = q.date.plusMinutes(i))
          case k: KibotPrice => k.copy(date = k.date.plusMinutes(i))
        }
      }
    }else Seq.empty[PriceTs]
  }


}
