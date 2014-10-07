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
){
	
	def printsth(b:SparkFlumeEvent): SparkFlumeEvent={
		var event = b
		val logger = Logger.getLogger("customLogger")
		event match{
					case _ if(event.event.getHeaders.toString contains("netflow")) => logger.info("processing with dnim")//; dnim_badips.process(event)
					case _ if(event.event.getHeaders.toString contains("syslog")) => logger.info("processing with csl")//; csl.process(event); logins.process(event);
					case _ if(event.event.getHeaders.toString contains("urgrep")) => logger.info("processing with grep")//; urlgrep_analyzer.process(event)
					}
		/*if(c.getHeaders.toString contains "syslog")
			logger.info("syslog")
		if(c.getHeaders.toString contains "netflow")
			logger.info("nflow")
		if(c.getHeaders.toString contains "urlgrep")
			logger.info("grep")
		*/b	
	}
	def dumpForEach(foo:RDD[SparkFlumeEvent]){
		var bar = foo.collect
		var i = 0
		val logger = Logger.getLogger("customLogger")
		for(ba<- bar){
			var baz = ba.event
			//logger.info(baz.getHeaders.toString + new String(baz.getBody.array))
			i+=1
		}	
		if(i > 0)logger.info(i+" foo")	
	}	
	def printEvent(b : RDD[Option[SparkFlumeEvent]]){
		val logger = Logger.getLogger("customLogger")
		var x = b.collect
		//logger.info(x)
		//println(x)
		var i = 0;
		for(d<-x){
			i+=1
			d match{case None=>
				case _=>var a = d.get.event
			logger.info("lalala printing "+a.getHeaders.toString + new String(a.getBody.array))
			println("lalala printing "+a.getHeaders.toString + new String(a.getBody.array))
		}}
		//if(i!=0)
		//logger.info(i+" foo")
	}
/*	def printAllMapped(element : SparkFlumeEvent): Option[AvroFlumeEvent] = {
		val logger = Logger.getLogger("customLogger")
		logins.process(element.event) match{
			case None => None
			case sth => return sth
		}
		csl.process(element.event) match{
			case None=> None
			case sth => return sth
		}
	*	dnim_badips.process(element.event) match {
			case None => None
			case sth => return sth
		}
	}
	def printAll(element : RDD[SparkFlumeEvent]){
		val logger = Logger.getLogger("customLogger")
		var data = element.collect
		var i = 0
		logger.info(1)
		for(d <- data){
			i+=1
			logger.info(d.event.getHeaders + new String(d.event.getBody.array))
		}
		if(i!=0)
		logger.fatal(i)
	}*/
}
