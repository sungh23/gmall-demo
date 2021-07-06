package com.admin.handle

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.admin.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author sungaohua
 */
object DauHandle {
  /**
   * 使用GroupBy对批次内数据进行去重
   * @param filterDStream
   * @return
   */
  def filterbyGroup(filterDStream: DStream[StartUpLog]) = {
    //转换数据结构  形成  （（mid，logdate），StartUpLog） K-V数据结构
    val midDateLog: DStream[((String, String), StartUpLog)] = filterDStream.map(startLog => {
      ((startLog.mid, startLog.logDate), startLog)
    })

    //使用groupByKey 并按时间排序取出第一条数据
    val groupDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateLog.groupByKey()

    val sortDStream: DStream[((String, String), List[StartUpLog])] = groupDStream.mapValues(iter => {
      iter.toList.sortBy(_.ts).take(1)
    })

    //将value数据取出 打散返回
    val startLogDStream: DStream[StartUpLog] = sortDStream.flatMap(_._2)
    startLogDStream
  }

  /**
   * 对redis中分区间数据去重
   *
   * @param startUpLogDStream
   * @return
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],spark:SparkContext) = {

    //方案一：
    /*val filterDS: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      //根据key查询数据
      val mids: util.Set[String] = jedis.smembers("DAU" + startUpLog.logDate)

      //判断是否包含,存在就过滤掉
      val bool: Boolean = mids.contains(startUpLog.mid)

      jedis.close()
      !bool
    })
    filterDS */

    //方案二 在每个分区中创建一个redis连接
    /*val filterDS: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val startUp: Iterator[StartUpLog] = partition.filter(startUpLog => {
        val mids: util.Set[String] = jedis.smembers("DAU" + startUpLog.logDate)

        val bool: Boolean = mids.contains(startUpLog.mid)
        !bool
      })
      jedis.close()
      startUp
    })*/

    //方案三  每个批次下只创建一次redis连接 使用广播变量的方式分发mids
    val filterDS: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {

      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      //手动拼写出key值

      val key: String = "DAU" + df.format(new Date())
      println(key)

      val mids: util.Set[String] = jedis.smembers(key)
      println(mids.size())

      //广播变量
      val midsBd: Broadcast[util.Set[String]] = spark.broadcast(mids)

      val value: RDD[StartUpLog] = rdd.filter(startlog => {
        !midsBd.value.contains(startlog.mid)
      })

      jedis.close()
      value
    })
    filterDS
  }

  /**
   * 将数据保存到redis中
   *
   * @param startUpLogDStream
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(startUpLogDS => {
      //执行在Driver端代码
      startUpLogDS.foreachPartition(iter => {
        //执行在Driver端代码 一个分区一次
        //        创建jedis连接  一个分区创建一个  减少连接的创建
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        iter.foreach(startUpLog => {
          //执行在EXE端代码
          //设计redis中key  key（Dau+logDate）
          val key: String = "DAU" + startUpLog.logDate
          // 放入到set集合中 同时设置一个小时过期时间
          jedis.sadd(key,startUpLog.mid)

          //设置过期时间
//          jedis.expire(key,3600)
        })
        //关闭连接
        jedis.close()
      })
    })
  }

  def main(args: Array[String]): Unit = {
    val date: Date = new Date()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    println(df.format(date))
  }
}
