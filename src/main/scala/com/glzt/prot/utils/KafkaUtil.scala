package com.glzt.prot.utils

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util
import java.util.Properties

/**
 * @Author:Tancongjian
 * @Date:Created in 14:53 2022/11/8
 *
 */
object KafkaUtil {


  def producer_time(table:String,time:String): Unit ={
    val prop = new Properties
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers", "192.168.108.27:9092,192.168.108.183:9092,192.168.108.228:9092")// 指定响应方式
    prop.put("acks", "all")
    // 请求失败重试次数
    //prop.put("retries", "3")
    // 指定key的序列化方式, key是用于存放数据对应的offset
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 配置超时时间
    prop.put("request.timeout.ms", "60000")
    // 得到生产者的实例
    val producer = new KafkaProducer[String, String](prop)
    //发送给kafka
    val data = Map[String, String](s"table" -> s"$table",s"published_at" -> s"$time")
    import scala.util.parsing.json.JSONObject
    val mapJsonStr = JSONObject.apply(data).toString()
    println(mapJsonStr)
    producer.send(new ProducerRecord[String, String]("slag_truck_top", mapJsonStr))
    producer.close()
  }


  def consumer_time(classed:Unit,name:String) {
    /**
     * 创建消费者
     */
    val properties = new Properties()
    //1、kafka broker列表
    properties.setProperty("bootstrap.servers", "192.168.108.27:9092,192.168.108.183:9092,192.168.108.228:9092")
    //2、指定消费者组
    //“group.id” 消费组 ID
    properties.setProperty("group.id", "slag_truck_realtime_1")
    properties.setProperty("enable.auto.commit", "false")
    //指定 key value 反序列化的类
    //序列化类型需要和生产者中一致
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //从最早读取数据
    //创建消费者
    val consumer = new KafkaConsumer[String, String](properties)
    val topics = new util.ArrayList[String]()
    //存入一个需要被订阅的topic
    topics.add("slag_truck_top")
    //需要传入一个集合对象
    consumer.subscribe(topics)
    //加上循环模拟一直消费
//    while (true) {
//      println("消费数据")
      //从kafka中拉取数据，一次拉多条数据
      //需要指定超时时间，1秒
      val records: ConsumerRecords[String, String] = consumer.poll(3000)
      //解析数据,获取指定topic的数据
      //传入topic -- student3
      val iterable = records.records("slag_truck_realtime")
      //返回一个迭代器，并拿到迭代器对象
      val iter: util.Iterator[ConsumerRecord[String, String]] = iterable.iterator()
      while (iter.hasNext) {
        //一行数据
        val record: ConsumerRecord[String, String] = iter.next()
        val value = record.value()
        val table = JSON.parseObject(value).get("table")
        val published_at = JSON.parseObject(value).get("published_at")
        if (table == s"$name"){
          println(table)
          println(published_at)
          classed
        }else{
          println("无该小时数据")
        }
      }
      consumer.commitSync()
//    }
  }


}
