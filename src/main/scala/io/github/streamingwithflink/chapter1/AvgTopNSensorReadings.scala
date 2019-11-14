package io.github.streamingwithflink.chapter1

import java.util
import java.util.PriorityQueue
import java.util.concurrent.ArrayBlockingQueue

import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 每5秒窗口出一个最大温度，然后统计10个连续最大温度值中TOP 3的平均最大温度
  */
object AvgTopNSensorReadings {

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
      .process(new AvgTopNKeyedProcessFunction(3, 2))

    deltaStram.print()

    // execute application
    env.execute("Compute delta temperature of maximum sensor temperatures in each contiguous window")
  }
}

case class SensorReadingAvgTopN(id: String, currentTime: Long, currentTemp: Double, avgTemp: Double)

class AvgTopNKeyedProcessFunction(candidateNum: Int, topN: Int) extends KeyedProcessFunction[String, SensorReading, SensorReadingAvgTopN] {

  private var candidateSensorReadingsState: ValueState[util.Queue[SensorReading]] = _

  override def open(parameters: Configuration): Unit = {
    val candidateSensorReadingsDescriptor = new ValueStateDescriptor[util.Queue[SensorReading]]("candidateSensorReadings", classOf[util.Queue[SensorReading]])
    candidateSensorReadingsState = getRuntimeContext.getState[util.Queue[SensorReading]](candidateSensorReadingsDescriptor)
  }

  def avgTopN(candidateSensorReadings: util.Queue[SensorReading], topN: Int) = {
    // 构造最大堆
    val maxHeap = new PriorityQueue[SensorReading](topN, (i1: SensorReading, i2: SensorReading) => ((i2.temperature - i1.temperature) * 1000).toInt)

    candidateSensorReadings.forEach(sensorReading => {
      // 插入元素进最大堆
      maxHeap.offer(sensorReading)
      // 超过topN个数则删除最大堆中的最小元素
      if (maxHeap.size() > topN) {
        val it = maxHeap.iterator()
        while(it.hasNext) {
          it.next()
        }
        it.remove()
      }
    })

    maxHeap.forEach(sensorReading => {
      println("heap >>> " + sensorReading)
    })

    val (cnt, sum) = maxHeap.toArray(new Array[SensorReading](maxHeap.size())).foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt
    avgTemp
  }

  override def processElement(currentSensorReading: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, SensorReadingAvgTopN]#Context,
                              out: Collector[SensorReadingAvgTopN]): Unit = {
    println()
    println()
    println("current >>> " + currentSensorReading)
    // fetch the last temperature from state
    var candidateSensorReadings: util.Queue[SensorReading] = candidateSensorReadingsState.value()
    if (candidateSensorReadings == null) {
      candidateSensorReadings = new ArrayBlockingQueue[SensorReading](candidateNum)
    }
    // check if we need to emit an alert
    if (candidateSensorReadings != null) {
      if (candidateSensorReadings.size() >= candidateNum) {
        candidateSensorReadings.poll()
      }
      candidateSensorReadings.add(currentSensorReading)

      val avgTemp = avgTopN(candidateSensorReadings, topN)
      out.collect(new SensorReadingAvgTopN(currentSensorReading.id, currentSensorReading.timestamp, currentSensorReading.temperature, avgTemp))
    }
    candidateSensorReadingsState.update(candidateSensorReadings)
  }

}