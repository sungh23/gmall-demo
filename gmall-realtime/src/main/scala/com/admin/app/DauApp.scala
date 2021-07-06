package com.admin.app

import java.text.SimpleDateFormat
import java.util.Date

import com.admin.bean.StartUpLog
import com.admin.constants.GmallConstants
import com.admin.handle.DauHandle
import com.admin.utils.MyKafkaUtil
import org.apache.phoenix.spark._
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author sungaohua
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //获取conf配置文件
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dauApp")
    // 获取ssc对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.获取kafka中StartUp（启动日志）主题的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将json数据转为样例类（StartUpLog）并且，补全LogDate（精确到天）和LogHour(精确到小时）字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(pattition => {
      pattition.map(record => {
        //使用fastjson转换成样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val dfTimes: Array[String] = sdf.format(new Date(startUpLog.ts)).split(" ")
        //补全LogDate 和LogHour
        startUpLog.logDate = dfTimes(0)
        startUpLog.logHour = dfTimes(1)

        startUpLog
      })
    })
//    startUpLogDStream.print()
    //优化：对DStream缓存
    startUpLogDStream.cache()

    //5.进行批次间去重
    val filterDStream: DStream[StartUpLog] = DauHandle.filterByRedis(startUpLogDStream, ssc.sparkContext)

    //优化：对DStream缓存
    filterDStream.cache()
    //打印原始数据条数
    startUpLogDStream.count().print()
    //打印经过批次间去重后的数据条数
    filterDStream.count().print()


    //6.进行批次内去重  使用GroupBy
    val groupDStream: DStream[StartUpLog] = DauHandle.filterbyGroup(filterDStream)
    //打印经过批次内去重后的数据条数
    groupDStream.cache()

    groupDStream.count().print()

    //7.将去重后的mid保存至Redis
    DauHandle.saveToRedis(groupDStream)

    //8.将去重后的明细数据保存至Hbase
    groupDStream.foreachRDD(startRDD=>{
      startRDD.saveToPhoenix("GMALL0225_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
