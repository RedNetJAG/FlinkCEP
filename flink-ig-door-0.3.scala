import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction
import org.apache.flink.streaming.api.scala._
import play.api.libs.json.{JsError, JsSuccess, Json}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import scala.collection.mutable
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

object rabbitjob {

  case class MyJson(ty: Int,
                    rc: String,
                    ss: String,
                    vl: Boolean,
                    cr: Long,
                    pr: Long,
                    dv: Long,
                    id: Long
                   )

  class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[(Long, String, String, Long)] {

    val maxTimeLag = 50000L; // 5 minutes

    override def extractTimestamp(element: (Long, String, String, Long), previousElementTimestamp: Long): Long = {
      element._1
    }

    override def getCurrentWatermark(): Watermark = {
      // return the watermark as current time minus the maximum time lag
      new Watermark(System.currentTimeMillis() - maxTimeLag)
    }
  }

  def main (args:Array[String]){

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readFileStream("hdfs:///test/ignicion-TS.io",100,FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED)

    def matchtype (x: Int): String = x match{
      case 4 => "Door"
      case 13 => "Ignition"
      case _ => "otros"
    }

    def matchvalue (x: Boolean): String = x match{
      case true => "ON"
      case false => "OFF"
    }

    val cc = stream.map(line => Json.parse(line)).map(json => Json.format[MyJson].reads(json).filter(s => (s.ty == 13 || s.ty == 4)).map(s => (s.cr, matchtype(s.ty), matchvalue(s.vl),s.dv)))
    val igd = cc.map(s =>{s match {case s: JsSuccess[(Long,String,String,Long)] => s.get case e: JsError => (654321L,"error","error",123L)} }).filter(s => s._3 != "error").keyBy(_._4)
    val timedValue = igd.assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator)

    // Ignition pattern
    val ig = timedValue.filter(_._2 == "Ignition")
    val patternIG_ON: Pattern[(Long,String,String,Long), _] = Pattern.begin[(Long,String,String,Long)]("start").where(_._3 == "OFF").next("end").where(_._3 == "ON")
    val patternStream: PatternStream[(Long,String,String,Long)] = CEP.pattern(ig, patternIG_ON)
    val patternIG_OFF: Pattern[(Long,String,String,Long), _] = Pattern.begin[(Long,String,String,Long)]("start").where(_._3 == "ON").next("end").where(_._3 == "OFF")
    val patternStreamIG_OFF: PatternStream[(Long,String,String,Long)] = CEP.pattern(ig, patternIG_OFF)

    // Door pattern
    val door = timedValue.filter(_._2 == "Door")
    val patternStreamDON: PatternStream[(Long,String,String,Long)] = CEP.pattern(door, patternIG_ON)
    val patternStreamDOFF: PatternStream[(Long,String,String,Long)] = CEP.pattern(door, patternIG_OFF)

    def selectFn(pattern : mutable.Map[String,(Long,String,String,Long)]): String = {
      val startEvent = pattern.get("start").get
      val endEvent = pattern.get("end").get
      "Device State Change: "+startEvent._1+" "+endEvent._1+":"+startEvent._2+" ON"
    }
    def selectFnOFF(pattern : mutable.Map[String,(Long,String,String,Long)]): String = {
      val startEvent = pattern.get("start").get
      val endEvent = pattern.get("end").get
      "Device State Change: "+startEvent._1+" "+endEvent._1+":"+startEvent._2+" OFF"
    }

    // Ignition
    val patternStreamSelected = patternStream.select(selectFn(_)).print()
    val patternStreamSelectedOFF = patternStreamIG_OFF.select(selectFnOFF(_)).print()
    // Door
    val patternStreamSelectedDOOR = patternStreamDON.select(selectFn(_)).print()
    val patternStreamSelectedDOOROFF = patternStreamDOFF.select(selectFnOFF(_)).print()


    // Ignition - Door pattern
    val patternTOT: Pattern[(Long,String,String,Long), _] = Pattern.begin[(Long,String,String,Long)]("start").where(_._2 == "Ignition").where(_._3 == "ON").next("end").where(_._2 == "Door").where(_._3 == "ON")
    val patternStreamTOTON: PatternStream[(Long,String,String,Long)] = CEP.pattern(timedValue, patternTOT)
    def selectFnTOT(pattern : mutable.Map[String,(Long,String,String,Long)]): String = {
      val startEvent = pattern.get("start").get
      val endEvent = pattern.get("end").get
      "ALERT: "+startEvent._1+" "+endEvent._1+" Door Open"
    }
    val patternStreamSelectedTOT = patternStreamTOTON.select(selectFnTOT(_)).print()

    env.execute("Test")
  }
}
