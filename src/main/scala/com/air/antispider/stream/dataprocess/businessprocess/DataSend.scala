package com.air.antispider.stream.dataprocess.businessprocess

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.dataprocess.constants.BehaviorTypeEnum
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

object DataSend {
  def sendQueryDataToKafka(processed: RDD[ProcessedData]) = {

      //过滤出query的数据
      val queryDataToKafka = processed.filter(y => y.requestType.behaviorType == BehaviorTypeEnum.Query).map(x => x.toKafkaString())
      //如果有查询数据，推送查询数据到Kafka
      if (!queryDataToKafka.isEmpty()) {
        //查询数据的topic：target.query.topic = processedQuery
        val queryTopic = PropertiesUtil.getStringByKey("target.query.topic", "kafkaConfig.properties")
        //创建map封装kafka参数
        val props = new java.util.HashMap[String, Object]()
        //设置brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
        //key序列化方法
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
        //value序列化方法
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))
        //批发送设置：32KB作为一批次或10ms作为一批次
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
        props.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))
        //按照分区发送数据
        queryDataToKafka.foreachPartition { records =>
          //每个分区创建一个kafkaproducer
          val producer = new KafkaProducer[String, String](props)
          //循环rdd
          records.foreach (record =>{
            //发送数据
            val push = new ProducerRecord[String, String](queryTopic,null,record)
            producer.send(push)
          }
          )
          //关闭流
          producer.close()
        }
      }
    }

  def sendBookDataToKafka(processed: RDD[ProcessedData]): Unit = {

    //过滤出book的数据
    val bookDataToKafka = processed.filter(y => y.requestType.behaviorType == BehaviorTypeEnum.Book).map(x => x.toKafkaString())
    //book的topic
    val bookTopic = PropertiesUtil.getStringByKey("target.book.topic", "kafkaConfig.properties")
    //推送预订数据到Kafka
    if (!bookDataToKafka.isEmpty()) {
      //创建map，封装kafka参数
      val props = new java.util.HashMap[String, Object]
      //设置brokers
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
      //key序列化方法
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
      //value序列化方法
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))
      //批发送设置：32KB作为一批次或10ms作为一批次
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
      props.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))
      //按照分区发送数据
      bookDataToKafka.foreachPartition  {records =>
        //每个分区创建一个kafkaproducer
        val producer = new KafkaProducer[String, String](props)
        records.foreach {record =>
          //发送数据
          val push = new ProducerRecord[String, String](bookTopic,null,record)
          producer.send(push)
        }
        //关闭流
        producer.close()
      }
    }
  }

}
