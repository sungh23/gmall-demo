package com.admin.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.admin.bean.{CouponAlertInfo, EventLog}
import com.admin.constants.GmallConstants
import com.admin.utils.{MyEsUtil, MyKafkaUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * @author sungaohua
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //    // 获取SparkConf 和StreamingContext
    //    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    //    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //
    //    // 1 获取kafka数据
    //    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    //
    //
    //    // 2 转换为样例类 补全数据
    //    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    //    val eventDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
    //      partition.map(record => {
    //        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
    //        // 补全数据
    //        eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
    //        eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)
    //
    //        //返回map（mid -> eventLog）类型数据
    //        (eventLog.mid, eventLog)
    //      })
    //    })
    //
    //    //开窗  五分钟一个窗口
    //    val windDStream: DStream[(String, EventLog)] = eventDStream.window(Minutes(5))
    //
    //    // 根据key聚合 （mid）
    //    val groupDStream: DStream[(String, Iterable[EventLog])] = windDStream.groupByKey()
    //
    //    //根据条件筛选出符合预警条件的事件
    //    /*
    //      用不同账号
    //      登录并领取优惠劵，
    //      并且过程中没有浏览商品
    //     */
    //    val boolToAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = groupDStream.mapPartitions(partition => {
    //      partition.map {
    //        case (mid, iter) => {
    //          // 创建set集合存放用户id
    //          val uids: util.HashSet[String] = new util.HashSet[String]()
    //          // 创建Set集合存放优惠卷id
    //          val itemIds: util.HashSet[String] = new util.HashSet[String]()
    //          // 创建list集合存放事件
    //          val events: util.ArrayList[String] = new util.ArrayList[String]()
    //
    //          //筛选数据  ，判断是否有浏览商品 有就跳过
    //          //初始化标志位
    //          var bool: Boolean = true;
    //          breakable {
    //            iter.foreach(log => {
    //              //将事件id放入到事件集合中
    //              events.add(log.evid)
    //              if ("clickItem".equals(log.evid)) {
    //                bool = false
    //                // 跳过有浏览商品的数据
    //                break()
    //              } else if ("coupon".equals(log.evid)) {
    //                // 优惠卷涉及的商品id
    //                itemIds.add(log.itemid)
    //                // 用户id
    //                uids.add(log.uid)
    //              }
    //            })
    //          }
    //
    //          //返回疑似预警数据
    //          (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    //
    //        }
    //      }
    //    })
    //
    //    //生成预警日志
    //    val couponDStream: DStream[CouponAlertInfo] = boolToAlertInfoDStream.filter(_._1).map(_._2)
    //
    //    couponDStream.print()
    //    //将数据写入到es中
    //    couponDStream.foreachRDD(rdd => {
    //      rdd.foreachPartition(iter => {
    //        val list: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
    //          (log.mid + log.ts / 1000 / 60, log)
    //        })
    //        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME, list)
    //      })
    //    })
    //
    //    //开启任务
    //    ssc.start()
    //    ssc.awaitTermination()

    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //转换位样例类 补全数据
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val mapDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全数据
        eventLog.logDate = sdf.format(eventLog.ts).split(" ")(0)
        eventLog.logHour = sdf.format(eventLog.ts).split(" ")(1)

        //需要根据mid分组  所以返回map（mid ，eventlog类型数据）
        (eventLog.mid, eventLog)
      })
    })

    // 筛选数据  5分钟内三次及以上 用不同账号 登录并领取优惠劵， 并且过程中没有浏览商品。
    // 根据key分组  key=mid
    val groupDStream: DStream[(String, Iterable[EventLog])] = mapDStream.groupByKey()

    // 开窗 窗口大小  5分钟
    val windDStream: DStream[(String, Iterable[EventLog])] = groupDStream.window(Minutes(5))

    // 筛选数据
    val boolDStream: DStream[(Boolean, CouponAlertInfo)] = windDStream.mapPartitions(partition => {

      partition.map {
        case (mid, iter) =>
          // 创建set集合保存uid
          val uids: util.HashSet[String] = new util.HashSet[String]()
          // 创建set集合保存商品id
          val itemIds: util.HashSet[String] = new util.HashSet[String]()
          // 创建list集合保存事件id
          val list: util.ArrayList[String] = new util.ArrayList[String]()

          //定义标志位  标志是否为未浏览商品数据
          var bool = true

          // breakable 包裹在for循环外 跳过相关数据
          breakable {
            iter.foreach(log => {
              //保存事件id
              list.add(log.evid)
              if ("clickItem".equals(log.evid)) {
                bool = false
                //浏览过商品 跳过
                break()
              } else if ("coupon".equals(log.evid)) {

                //保存商品id 用户id
                uids.add(log.uid)
                itemIds.add(log.itemid)
              }
            })
          }
          // 返回疑似报警数据
          (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, list, System.currentTimeMillis()))
      }
    })

    // 取出报警数据
    val filterDStream: DStream[CouponAlertInfo] = boolDStream.filter(_._1).map(_._2)

    filterDStream.print()

    // 存入到es中
    filterDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val coupon: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME,coupon)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }


}
