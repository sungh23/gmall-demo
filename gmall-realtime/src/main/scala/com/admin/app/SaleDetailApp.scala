package com.admin.app

import java.text.SimpleDateFormat
import java.util

import com.admin.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.admin.constants.GmallConstants
import com.admin.utils.{MyEsUtil, MyKafkaUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._

/**
 * @author sungaohua
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    // 获取连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 获取kafka数据  user_info
    val kfkInfoDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    // 获取kafka数据  order_detail
    val kfkDetailDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    // 转换成样例类 order_info

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val infoDStream: DStream[(String, OrderInfo)] = kfkInfoDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = sdf.format(orderInfo.create_time).split(" ")(0)
        orderInfo.create_hour = sdf.format(orderInfo.create_time).split(" ")(1)
        (orderInfo.id, orderInfo)
      })
    })

    // 转换成样例类  order_detail
    val detailDStream: DStream[(String, OrderDetail)] = kfkDetailDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    // 使用fullOuterJoin的方式获取到该批次中所有的数据
    val fullJOINDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = infoDStream.fullOuterJoin(detailDStream)

    // 使用redis缓存的方式解决网络波动带来的数据丢失问题
    val noUserInfoDStream: DStream[SaleDetail] = fullJOINDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //传教redis连接
      val redis: Jedis = new Jedis("hadoop102", 6379)
      // 创建list集合保存SaleDetail数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      partition.foreach {
        case (orderId, (infoOpt, detailOpt)) => {
          // 设计redis中key
          val orderKey: String = "orderInfo" + orderId
          val detailKey: String = "orderDetail" + orderId

          // 判断info数据是否存在
          if (infoOpt.isDefined) {

            val orderInfo: OrderInfo = infoOpt.get
            // 判断detail是否存在
            if (detailOpt.isDefined) {
              // 同时存在 写入到SaleInfo中
              val saleDetail: SaleDetail = new SaleDetail(orderInfo, detailOpt.get)
              details.add(saleDetail)
            }

            // 将样例类转换为json类型数据存入到redis中
            val orderJson: String = Serialization.write(orderInfo)
            // 保存到redis中 设置过期时间为10秒
            redis.set(orderKey, orderJson)
            redis.expire(orderKey, 10)

            // 查询redis中是否有detail数据
            if (redis.exists(detailKey)) {
              // 存在
              val detailSet: util.Set[String] = redis.smembers(detailKey)
              detailSet.asScala.foreach(detail => {
                //将查询出来的json结果转换为样例类
                val orderDetail: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
                val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                details.add(saleDetail)
              })
            }
          } else {
            //orderInfo数据不存在  detailOpt存在
            if (detailOpt.isDefined) {
              // 从redis中查询orderinfo数据 join 放入list中
              if (redis.exists(orderKey)) {
                val orderInfoStr: String = redis.get(orderKey)
                val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])

                val detail: SaleDetail = new SaleDetail(orderInfo, detailOpt.get)
                details.add(detail)
              } else {
                //没有join上  将detailInfo的数据放入到redis中
                val orderDetail: OrderDetail = detailOpt.get
                //转换为json字符串
                val orderDetailStr: String = Serialization.write(orderDetail)
                //存入redis数据并设置过期时间
                redis.set(detailKey, orderDetailStr)
                redis.expire(detailKey,20)
              }
            }

          }

        }
      }

      // 关闭redis连接
      redis.close()

      //返回一个迭代器
      details.asScala.iterator
    })

    // 补全userInfo数据
    val userInfoDStream: DStream[SaleDetail] = noUserInfoDStream.mapPartitions(partition => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      partition.map(saleDetail => {
        // 转换为样例类   拼出key值
        val key: String = "userInfo" + saleDetail.user_id

        //取出数据  转换为样例类
        val userInfoStr: String = jedis.get(key)
        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])

        // 合并数据
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
    })
    userInfoDStream.print()

    //将数据写入到Es中
    userInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(sale => (sale.order_detail_id, sale))
        //将数据写入到es中
        MyEsUtil.insertBulk(GmallConstants.ES_DETAIL_INDEXNAME,list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
