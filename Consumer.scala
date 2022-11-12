
// TODO : Comment code block 
////////////////////////////
import java.util.Properties
import kafkashaded.org.apache.kafka.common.serialization.StringDeserializer

val kafkaProps = new Properties()
  
kafkaProps.clear
kafkaProps.put("bootstrap.servers", "XX.XX.XXX.XX:XXXX,XX.XX.XXX.XX:XXXX")
kafkaProps.put("ssl.security.protocol","SSL")
kafkaProps.put("key.deserializer","kafkashaded.org.apache.kafka.common.serialization.StringDeserializer");  
kafkaProps.put("value.deserializer","kafkashaded.org.apache.kafka.common.serialization.StringDeserializer");
kafkaProps.put("group.id", "ConsumerGroup1");


val consumer = new KafkaConsumer(kafkaProps)
val topics = List("YOUR_TOPIC_NAME_HERE")
  
try {
  consumer.subscribe(topics.asJava)
  while (true) {
    val records = consumer.poll(Duration.ofMillis(100))
    for (record <- records.asScala) {
      println("Topic: " + record.topic() + 
               ",Key: " + record.key() +  
               ",Value: " + record.value() +
               ", Offset: " + record.offset() + 
               ", Partition: " + record.partition())
    }
  }
}catch{
   case e:Exception => e.printStackTrace()
}finally {
   consumer.close()
}
