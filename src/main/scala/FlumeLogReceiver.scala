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
	val csl: CertLogProcessor,
	val logins: LoginProcessor,
	val dnim_badips:  CertLogProcessor
	){
	
/*	def printsth(b:SparkFlumeEvent): SparkFlumeEvent={
		var c = b.event
		val logger = Logger.getLogger("customLogger")
		logger.info("Found: " + c.getHeaders.toString + new String(c.getBody.array))
		b
	}
	def dumpForEach(foo:RDD[SparkFlumeEvent]){
		var bar = foo.collect
		var i = 0
		val logger = Logger.getLogger("customLogger")
		logger.info("printing " + bar)
		for(ba<- bar){
			var baz = ba.event
			logger.info(baz.getHeaders.toString + new String(baz.getBody.array))
			i+=1
		}	
			logger.info(i+" foo")
	}
*/	def printEvent(b : RDD[Option[AvroFlumeEvent]]){
		val logger = Logger.getLogger("customLogger")
		var x = b.collect
		//logger.info(x)
		//println(x)
		var i = 0;
		for(d<-x){
			i+=1
			d match{case None=>
				case _=>var a = d.get
			logger.info("lalala printing "+a.getHeaders.toString + new String(a.getBody.array))
			println("lalala printing "+a.getHeaders.toString + new String(a.getBody.array))
		}}
		if(i!=0)
		logger.info(i+" foo")
	}
	def printAllMapped(element : SparkFlumeEvent): Option[AvroFlumeEvent] = {
		val logger = Logger.getLogger("customLogger")
	/*	logins.process(element.event) match{
			case None => None
			case sth => return sth
		}
		csl.process(element.event) match{
			case None=> None
			case sth => return sth
		}
	*/	dnim_badips.process(element.event) match {
			case None => None
			case sth => return sth
		}
	}
	def printAll(element : RDD[SparkFlumeEvent]){
		val logger = Logger.getLogger("customLogger")
		var data = element.collect
		var i = 0
		for(d <- data){
			i+=1
			logger.info(d.event.getHeaders + new String(d.event.getBody.array))
		}
		if(i!=0)
		logger.fatal(i)
	}
}
