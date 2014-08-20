import org.apache.spark.SparkConf
import org.apache.spark.rdd._
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
	
	def printAll(element : RDD[SparkFlumeEvent]){
		val logger = Logger.getLogger("customLogger")
		var data = element.collect
		var i = 0
		for(d <- data){
			logger.fatal(processor.process(d.event))
			i+=1
			//logger.fatal("Printing data for RDD " + new String(d.event.getBody.array))
		}
		logger.fatal(i)
	}

	def run(){

		println("WIll Run on " +this.host + " port " + this.port+" reading every "+this.readingFreq+"ms")

		val logger = Logger.getLogger("customLogger")
		
		val ssc = new StreamingContext(this.conf,Milliseconds(this.readingFreq))
		val stream = FlumeUtils.createStream(ssc, this.host, this.port,StorageLevel.MEMORY_AND_DISK_2)

//		stream.repartition(1)
//		stream.count.map(cnt=>"Received " +cnt+" events").print
//		var test = stream.count
//		test.map(cnt => "Count is of type "+cnt.getClass.getName).print
		


//	 	var cnt = stream.countByValue()
//		cnt.map(cnt => "Count: Received " + cnt + " flume events." ).print
//		stream.filter(event=> new String(event.event.getBody.array) contains "google").print
	
		stream.foreachRDD(foo => printAll(foo))
		/* And that's the correct way to transform the event to a string */
		//stream.map(foo => logger.fatal(new String(foo.event.getBody.array)) ).print
	


		//stream.map(event=>"Event: header:"+ event.event.getHeaders.toString+" body:"+ new String(event.event.getBody.array) ).print

		//var bench = stream.map(event=>processor.benchmark(event.event))
		//bench.filter(element=> element.get match{case "None found" => return false case _=> return true}).print
	
		//var out = stream.map(event => processor.process(event.event))
		//out.print
		
	
	
		//stream.foreachRDD(sth=>sth.foreach(event=>processor.process(event.event)))
		//stream.foreachRDD(sth=>logger.fatal(sth.collect.toString))
		//logger.info("Testing the logger, out== " + out.toString)		
//		logger.debug("Testing the logger, out== " + out.toString)		
	
		//ayto den kanei match tpt!
//		out.filter(element =>element match{case None=>false
//						   case Some(v)=> true /*v.getHeaders.containsValue("Monitoring")*/}).map(element=>"Filtered:" + element).print
		
		//stream.print()
		ssc.start()
		ssc.awaitTermination()
		//Thread.sleep(100000)
		//ssc.stop()
  }
}
