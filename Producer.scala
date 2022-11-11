//////////////////////
// import libraries //
//////////////////////
import java.util.Properties
import kafkashaded.org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter
import scala.concurrent.Promise
import kafkashaded.org.apache.kafka.clients.producer
import kafkashaded.org.apache.kafka.clients.producer._
import kafkashaded.org.apache.kafka.common.serialization.StringSerializer
import javax.net.ssl.KeyManagerFactory

////////////////////////////////
// Define your kafka message  //
////////////////////////////////
val jsonstring =
        s"""{
   | "accountNo": "1111111",
   | "fName": "Kennedy",
   | "lName": "McLeod",
   | "age": "17",
   | "address": "2 John St",
   | "city": "Toronto",
   | "prov": null,
   | "date": null,
   | "country": "Canada
            |}
         """.stripMargin

//////////////////////////////
// Print your Kafka Message //
//////////////////////////////
println(jsonstring)


///////////////////////////
// Kafka Producer Class  //
///////////////////////////
case class KafkaProducerConfig(topic: String, kafkaMsg:String, sync: Boolean) {
  val kafkaProps = new Properties()
  kafkaProps.clear
  /////////////////////////////////////////
  // Define your bootstrap kafka servers //
  /////////////////////////////////////////
  kafkaProps.put("bootstrap.servers", "XX.XX.XXX.XX:XXXX,XX.XX.XX.XX:XXXX")
  println("--------------")
  println(topic)
  println(kafkaMsg)
  println("--------------")
  /////////////////////////////////////////////////////
  // Configure your ssl, JKS truststore and password //
  /////////////////////////////////////////////////////
  kafkaProps.put("ssl.security.protocol","SSL")
  kafkaProps.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("ssl.truststore.location", "LOCATION of your.truststore.jks")
  kafkaProps.put("ssl.truststore.password", "password")
  kafkaProps.put("ssl.keystore.location", "LOCATION of your.truststore.jks")
  kafkaProps.put("ssl.keystore.password", "password")
  kafkaProps.put("ssl.key.password", "password")
  
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Configure Kafka Integration                                                                                               //
  // When Kafka acks is 1, the producer receives an acknowledgment as soon as the leader replica has received the message.     //
  // producer type - sync vs async (Passed in class )                                                                          //
  // how many times to retry when produce request fails?                                                                       //
  // linger.ms = number of millisec producer will wait before sending. The default value is 0, which means send msg right away //
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  kafkaProps.put("acks", "1")
  kafkaProps.put("producer.type", if(sync) "sync" else "async")
  kafkaProps.put("retries", "3")
  kafkaProps.put("linger.ms", "5")
  
  ////////////////////////////////////////////
  //this is our actual connection to Kafka! //
  ////////////////////////////////////////////
  private val producer = new KafkaProducer[String, String](kafkaProps)
  
  //////////////////////////////////////////
  // Call send function with kafka mesage //
  //////////////////////////////////////////
  send(jsonstring)
  
  ///////////////////////////////////////////////////////////
  // Splitter function - forks on sync boolean to send msg //
  // Syncronously or Asyncronouly                          //
  ///////////////////////////////////////////////////////////
  def send(value: String): Unit = {
    if(sync) sendSync(value) else sendAsync(value)
  }
  
  //////////////////////////
  // Syncronous Sync Send //
  //////////////////////////
  def sendSync(value: String): Unit = {
    /////////////////////////////////////////
    // Create kafka record topic + message //
    /////////////////////////////////////////
    val record = new ProducerRecord[String, String](topic, value)
    //////////////////////////////////////////////
    // Try sending the message and catch errors //
    //////////////////////////////////////////////
    try {
      producer.send(record).get()
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }

  ///////////////////////////
  // Asyncronous Sync Send //
  ///////////////////////////
  def sendAsync(value: String):Unit = {
    /////////////////////////////////////////
    // Create kafka record topic + message //
    /////////////////////////////////////////
    val record = new ProducerRecord[String, String](topic, value)
    val p = Promise[(RecordMetadata, Exception)]()
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        p.success((metadata, exception))
      }
    })

  }

  def close():Unit = producer.close()
}


////////////////////////////////
// try producing your message //
////////////////////////////////
   try{
        // Call the kafka producer, specify the topic, the message and sync or async
        KafkaProducerConfig("your_topic_name_here",jsonstring, true);
      }
    catch{
        // handled exceptions
        case _: Throwable => println("Error creating Kafka Event ---- exception")
      }
