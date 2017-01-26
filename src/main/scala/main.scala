import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import com.cloudera.sparkts.models.ARIMA
import com.cloudera.sparkts.{EasyPlot, IrregularDateTimeIndex, TimeSeriesRDD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Artem on 25.01.2017.
  */
object main {
  def main(args: Array[String]): Unit = {
    println("test")

    val cfg = new SparkConf().setAppName("Diploma").setMaster("local")
    val sc = new SparkContext(cfg)

    val lines = sc.textFile("d:/green.tsv")

//    lines.take(10).foreach(println)
    val N = 10;

    val r1 = lines.mapPartitionsWithIndex { case (idx, iter) =>
      if (idx == 0) iter.drop(N) else iter
    }

    val r2 = r1.map(line => line.split("\t"))
    val r3 = r2.mapPartitions { elements =>
      val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
      elements.map { case Array(code, date, value) =>
        Array(code,
          LocalDateTime.parse(date,
            dateFormatter).atZone(ZoneId.systemDefault()),
          try {
            value.toDouble
          } catch {
            case e: Exception => Double.NaN
          })
      }
    }

    val timestamps = r3.map {
      case Array(code, date: ZonedDateTime, value: Double) =>
        date.toEpochSecond()
    }.distinct().sortBy(x => x)

    val dateTimeIndex = new
        IrregularDateTimeIndex(timestamps.collect())

    val r4 = r3.map { case Array(code: String, date: ZonedDateTime,
    value: Double) => (code, (date, value))
    }

    val r5 = r4.groupByKey()

    val r6 = r5.map { case (code, values) =>
      (code,
        values.toArray.sortWith {
          case ((d1: ZonedDateTime, v1), (d2: ZonedDateTime, v2)) =>
            0 > d1.compareTo(d2)
        })
    }

    val r7 = r6.map { case (code, values) => (code, values.map { case
      (date, value) => value
    })
    }

    val r8 = r7.map { case (code, values) => (code,
      Vectors.dense(values))
    }

    val tsRDD = new TimeSeriesRDD(dateTimeIndex, r8)

    val sampled = tsRDD.mapSeries { vector =>
      val sampledArray = vector.toArray.zipWithIndex.filter {
        case (value, idx) => idx % 1 == 0
      }

      Vectors.dense(sampledArray.map { case (value, idx) =>
        value
      })
    }

    /////////ARIMA MODEL

    val forecast15 = sampled.fill("linear").mapSeries(vector =>

      ARIMA.fitModel(1, 0, 1, vector).forecast(vector, 5))

    EasyPlot.ezplot(sampled.findSeries("green0&08"))
    EasyPlot.ezplot(forecast15.findSeries("green0&08"))
    //forecast15.saveAsTextFile("file://с:/forecast15")

  }
}
