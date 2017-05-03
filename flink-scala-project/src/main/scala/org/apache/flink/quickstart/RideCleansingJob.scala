package org.apache.flink.quickstart

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils.isInNYC
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object RideCleansingJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val Array(inputPath, maxDelay, speed) = args

    println(s"Program has started with following args: $inputPath, $maxDelay, $speed")

    env.addSource(new TaxiRideSource(inputPath, maxDelay.toInt, speed.toInt))
      .filter(tr => isInNYC(tr.startLon, tr.startLat) && isInNYC(tr.endLon, tr.endLat))
      .print()

    env.execute("ride-cleansing-job")
  }
}