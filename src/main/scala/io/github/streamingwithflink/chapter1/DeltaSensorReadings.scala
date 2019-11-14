package io.github.streamingwithflink.chapter1

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object DeltaSensorReadings {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val maxTempStream: DataStream[SensorReading] = sensorData
//      .filter(_.id.equals("sensor_28"))
      // convert Fahrenheit to Celsius using an inlined map function
      .map( r => SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      // organize stream by sensorId
      .keyBy(_.id)
      // group readings in 5 second windows
      .timeWindow(Time.seconds(5))
      // compute max temperature using a user-defined function
      .maxBy("temperature")

    val deltaStram = maxTempStream
      .keyBy(_.id)
      .process(new DeltaKeyedProcessFunction)

    deltaStram.print()

    // execute application
    env.execute("Compute delta temperature of maximum sensor temperatures in each contiguous window")
  }
}

case class SensorReadingDelta(id: String, lastTime: Long, lastTemp: Double, currentTime: Long, currentTemp: Double, deltaTemp: Double)

class DeltaKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, SensorReadingDelta] {

  private var lastSensorReadingState: ValueState[SensorReading] = _

  override def open(parameters: Configuration): Unit = {
    // register state for last sensorReading
    val lastSensorReadingDescriptor = new ValueStateDescriptor[SensorReading]("lastSensorReading", classOf[SensorReading])
    lastSensorReadingState = getRuntimeContext.getState[SensorReading](lastSensorReadingDescriptor)
  }

  override def processElement(currentSensorReading: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, SensorReadingDelta]#Context,
                              out: Collector[SensorReadingDelta]): Unit = {
    println(currentSensorReading)
    // fetch the last temperature from state
    val lastSensorReading = lastSensorReadingState.value()
    // check if we need to emit an alert
    if (lastSensorReading != null) {
      val tempDiff = (currentSensorReading.temperature - lastSensorReading.temperature).abs
      out.collect(new SensorReadingDelta(currentSensorReading.id, lastSensorReading.timestamp, lastSensorReading.temperature, currentSensorReading.timestamp, currentSensorReading.temperature, tempDiff))
    }
    lastSensorReadingState.update(currentSensorReading)
  }

}