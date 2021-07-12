package com.admin.app

import com.admin.bean.UserInfo
import com.admin.constants.GmallConstants
import com.admin.utils.MyKafkaUtil
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 *  从kafka消费数据  并将数据写入到redis缓存中
 *
 * @author sungaohua
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    // 获取连接
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 消费kafka数据 并写入到redis缓存中
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val redis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(record=>{
          val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          //设计存入redis的key
          val key: String = "userInfo"+userInfo.id
          redis.set(key,record.value())
        })
        //关闭redis连接
        redis.close()
      })
    })



    // 执行并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
