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
import org.apache.flume.source.avro._

@serializable
class FlumeLogReceiver(
	val processor: CertLogProcessor
	){
	def run(){

	//	println("WIll Run on " +this.host + " port " + this.port+" reading every "+this.readingFreq+"ms")

	//	val logger = Logger.getLogger("customLogger")
		
		//		stream.repartition(1)
//		stream.count.map(cnt=>"Received " +cnt+" events").print
		/* and that's the correct way to transform the event to a string */
//		stream.map(foo => new String(foo.event.getBody.array)).print
	
//		var test = stream.count
//		test.map(cnt => "Count is of type "+cnt.getClass.getName).print



//	 	var cnt = stream.countByValue()
//		cnt.map(cnt => "Count: Received " + cnt + " flume events." ).print
//		stream.filter(event=> new String(event.event.getBody.array) contains "google").print
	
		//stream.foreachRDD(foo => printAll(foo))


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
		
//		stream.print()
	//Thread.sleep(100000)
		//ssc.stop()
  	}
	def printEvent(b : RDD[Option[AvroFlumeEvent]]){
		val logger = Logger.getLogger("customLogger")
		var x = b.collect
		logger.info(x)
		println(x)
		var i = 0;
		for(d<-x){
			i+=1
			d match{case None=>
				case _=>var a = d.get
			logger.info("lalala printing "+a.getHeaders.toString + new String(a.getBody.array))
			println("lalala printing "+a.getHeaders.toString + new String(a.getBody.array))
		}}
		logger.info(i+" foo")
	}
	def printAllMapped(element : SparkFlumeEvent): Option[AvroFlumeEvent] = {
		val logger = Logger.getLogger("customLogger")
		processor.process(element.event) match{
			case None=> None
			case sth => return sth
		}
	}
	def printAll(element : RDD[SparkFlumeEvent]){
		val logger = Logger.getLogger("customLogger")
		var data = element.collect
		var i = 0
		for(d <- data){
			i+=1
			logger.info(processor.process(d.event))
		}
		logger.fatal(i)
	}
}
