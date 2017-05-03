package org.apache.flink.quickstart

import java.time.Instant
import java.time.Instant.ofEpochMilli

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time.minutes
import org.apache.flink.util.Collector

object PopularPlacesJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val Array(inputPath, maxDelay, speed) = args

    val popularityThreshold = 20

    println(s"Program has started with following args: $inputPath, $maxDelay, $speed")

    env.addSource(new TaxiRideSource(inputPath, maxDelay.toInt, speed.toInt))
      .filter(tr => isInNYC(tr.startLon, tr.startLat) && isInNYC(tr.endLon, tr.endLat))
      .map(tr => (tr.isStart, mapToGridCell(tr.startLon, tr.startLat)))
      .keyBy(x => x)
      .timeWindow(minutes(15), minutes(5))
      .apply( (key, window, items, collector: Collector[(Instant, Boolean, Float, Float, Int)]) =>
        collector.collect(
          (ofEpochMilli(window.getEnd), key._1, getGridCellCenterLon(key._2), getGridCellCenterLat(key._2), items.size)
        )
      )
      .filter(_._5 > popularityThreshold)
      .map(t => s"time=${t._1} start=${t._2} lon=${t._3} lat=${t._4} count=${t._5}")
      .print()

    env.execute("popular-places-job")
  }
}