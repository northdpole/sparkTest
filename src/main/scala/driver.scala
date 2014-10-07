import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext.rddToFileName
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
import java.io._
import java.util.Calendar
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout
import org.apache.spark.rdd._

object Driver {

	val hadoopPath = "/tmp/sparkResults/"

	/* setup logging */
	val console = new ConsoleAppender(); //create appender

	//configure the appender
	val PATTERN = "%d [%p|%c|%C{1}] %m%n";
	console.setLayout(new PatternLayout(PATTERN)); 
	console.setThreshold(Level.DEBUG);
	console.activateOptions();

	//add appender to any Logger (here is root)
	var logger = Logger.getLogger("customLogger")
	Logger.getLogger("customLogger").addAppender(console);

	
	val host: String = "localhost"
	val port: Int = 3564
	val readingFreq: Int = 400
	var flumeConnector :FlumeConnector = null
	var flumeLoginConnector: FlumeConnector = null	
	
	def main(args: Array[String]) {
		
	val csl = new CertLogProcessor("csl",null)
	val dnim_badips= new CertLogProcessor("dnim-badips",null)
	val urlgrep_analyzer= new CertLogProcessor("urlgrep-analyser",null)
	val logins = new LoginProcessor(/*"devdb11","dakMen5ojVotKoc+swev"*/)

	csl.set_config(csl.read_config(CertLogProcessor.config_path))
	dnim_badips.set_config(dnim_badips.read_config(CertLogProcessor.config_path))

	val today = Calendar.getInstance().getTime();
	println(today.toString)
	val conf = new SparkConf().setAppName(today.toString)
							  .set("spark.cleaner.ttl","15")
							  .set("spark.task.maxFailures","2")

	val receiver = new FlumeLogReceiver(/*csl,logins,dnim_badips,urlgrep_analyzer*/)
	val ssc = new StreamingContext(conf,Milliseconds(readingFreq))
	val stream = FlumeUtils.createStream(ssc, host, port,StorageLevel.MEMORY_AND_DISK_2)
	
/*	stream.map(event=> event match{
					case _ if event.event.getHeaders.toString contains("syslog") =>  logins.process(event)
					}).filter(event=> event match{
								case None => false
								case _ => true
					}).foreachRDD(rdd => saveFunction(rdd))
*/

	stream.map(event=> event match{
					case _ if event.event.getHeaders.toString contains("netflow") => dnim_badips.process(event)
					case _ if event.event.getHeaders.toString contains("syslog") =>  csl.process(event)
					case _ if event.event.getHeaders.toString contains "urlgrep"  => urlgrep_analyzer.process(event)
					}).filter(event=> event match{
								case None => false
								case _ => true
					}).foreachRDD(rdd => saveFunction(rdd))

	ssc.start()
	ssc.awaitTermination()
  }
  def saveFunction(rdd: RDD[Option[CERNcertSparkFlumeEvent]]) = {
	var x = 0
	val logger = Logger.getLogger("customLogger")
	var data = rdd.collect
	if(this.flumeConnector == null)
		this.flumeConnector = this.initConnector("alert")
	if(this.flumeLoginConnector == null)
		this.flumeLoginConnector = this.initConnector("login")


	if(rdd.count != 0){
		for(event<-data){
			logger.info(event.get.event.getHeaders.get("statementType"))
			if(event.get.event.getHeaders.get("eventType") == "login")
				this.flumeLoginConnector.send(new String(event.get.event.getBody.array), event.get.event.getHeaders)	
			else
				this.flumeConnector.send(new String(event.get.event.getBody.array), event.get.event.getHeaders)	
	
		}
	}
	
  }
  def initConnector(contype : String): FlumeConnector = {
		while(true){
			try{
				contype match{
					case "alert" =>	var connector = new FlumeConnector("localhost",41414)
							this.logger.info("Succesfully connected to flume agent")
							return connector
					case "login" =>	var connector = new FlumeConnector("localhost",41415)
							this.logger.info("Succesfully connected to flume agent")
							return connector
					case _	     => this.logger.fatal("Trying to initialize the logger with a wrong type, you have a bug")
				}
			}catch{
				case e: java.lang.ExceptionInInitializerError => this.logger.fatal("Can't Connect to flume agent, please make sure that flume is running")
											Thread.sleep(30)		
					case f:	org.apache.flume.FlumeException =>  this.logger.fatal("Cannot Connect to flume agent, please make sure that flume is running")
										Thread.sleep(300)		
			}
		}
		null
	}

 def containsSpec(text: List[Char], needle: String): Boolean = text match{
          case _ if text.contains(needle) => true
          case _ => false
      }
	
}
