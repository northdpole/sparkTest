import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
import org.apache.spark.storage.StorageLevel
import java.io._
import java.nio._

class FlumeLogReceiver (
	val conf: SparkConf,
	val readingFreq: Long,
	val processor: CertLogProcessor,
	val host: String,
	val port: Int
	 ){


	def run(){

		val ssc = new StreamingContext(this.conf,Milliseconds(this.readingFreq))
		val stream = FlumeUtils.createStream(ssc, this.host, this.port)
		//println(this.host+this.port)
		stream.repartition(10)
	 	stream.count.map(cnt => "Count: Received " + cnt + " flume events." ).print

		//stream.map(event=>"Event: header:"+ event.event.getHeaders.toString+" body:"+ new String(event.event.getBody.array) ).print
		
		var out = stream.map(event => processor.process(event.event))
		
		//.saveAsTextFiles("file:///tmp/spark/Output","Log")
		
		out.filter(element => element.get match{case "None found" => false case _=>true}).print
		//stream.print()
		ssc.start()
		ssc.awaitTermination()
		//Thread.sleep(100000)
		//ssc.stop()
  }
}
