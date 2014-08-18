import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
import org.apache.spark.storage.StorageLevel
import java.io._
import java.nio._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.BasicConfigurator

@serializable
class FlumeLogReceiver(
	val conf: SparkConf,
	val readingFreq: Long,
	val processor: CertLogProcessor,
	val host: String,
	val port: Int
	){


	def run(){

		println("WIll Run on " +this.host + " port " + this.port+" reading every "+this.readingFreq+"ms")

		val logger = Logger.getLogger("customLogger")
		
		val ssc = new StreamingContext(this.conf,Milliseconds(this.readingFreq))
		val stream = FlumeUtils.createStream(ssc, this.host, this.port)
		stream.repartition(3)
	 	//stream.count.map(cnt => "Count: Received " + cnt + " flume events." ).print

		//stream.map(event=>"Event: header:"+ event.event.getHeaders.toString+" body:"+ new String(event.event.getBody.array) ).print

		//var bench = stream.map(event=>processor.benchmark(event.event))
		//bench.filter(element=> element.get match{case "None found" => return false case _=> return true}).print
	
		var out = stream.map(event => processor.process(event.event))

						

		//logger.info("Testing the logger, out== " + out.toString)		
//		logger.debug("Testing the logger, out== " + out.toString)		
	
		//ayto den kanei match tpt!
		out.filter(element =>element match{case None=>false
						   case Some(v)=> v.getHeaders.containsValue("Monitoring")}).map(element=>"Filtered:" + element).print
		
		//stream.print()

		ssc.start()
		ssc.awaitTermination()
		//Thread.sleep(100000)
		//ssc.stop()
  }
}
