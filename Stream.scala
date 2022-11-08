///////////////////////////////////////////////////////////////////////
// Scala Kafka Stream written in DataBricks and separated by Cells   //
// Databricks and Splitting is no required                           //
///////////////////////////////////////////////////////////////////////

///////////////////////////////////////// CELL ONE ////////////////////////////////////////////////////////
// Define you Kafka Stream                                                                               //
// kafka.bootstrap.servers 12.12.123.12:1111  <- your bootstrap server and port                          //
// subscribe : topic_to_subscriber <- this is the kafka topic your stream will subscribe to and monitor  //
// Offset : start at the earliest Offset                                                                 //
///////////////////////////////////////////////////////////////////////////////////////////////////////////
val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "XX.XX.XXX.XX:XXXX")
        .option("subscribe", "sample-topic-name")
        .option("startingOffsets", "earliest") // From starting
        .load()

df.printSchema()

//////////////////////////////////// CELL TWO //////////////////////////////
// Since the value is in binary,                                          //
// first we need to convert the binary value to String using selectExpr() //
////////////////////////////////////////////////////////////////////////////
val personStringDF = df.selectExpr("CAST(value AS STRING)")

//////////////////////////////////// CELL THREE /////////////////////////////
// Now, extract the value which is in JSON String to 
// DataFrame and convert to DataFrame columns using custom schema.
////////////////////////////////////////////////////////////////////////////
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val schema = new StructType()
      .add("field1",IntegerType)
      .add("field2",StringType)
      .add("field3",StringType)
      .add("etc.....",StringType)

val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
   .select("data.*")

//////////////////////////////////// CELL FOUR  ////////////////////////////////
// Fire up the stream.  It will consume all messages sent to the defined topic.
////////////////////////////////////////////////////////////////////////////////

personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
