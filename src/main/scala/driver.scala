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
import scala.collection.JavaConverters

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout

import org.apache.spark.rdd._

import org.yaml.snakeyaml.Yaml 
import java.util.HashMap
import java.util.ArrayList
import CERNcertSparkFlumeEvent._

object Driver {
	

	val defaultConfig : String = """
'Streaming-Sources':
    'Sources':
        - 'localhost:3564'
'Filters':
    'csl':
        - 'ips'
        - 'domains'
        - 'accounts'
        - 'programs'
        - 'patterns'
    'dnim-badips':
        - 'domains'
    'urlgrep-analyzer':
        - 'domains'

'Spark Streaming Settings':
    'Read_Interval':
        - 400
    'spark.cleaner.ttl':
        - 15
    'spark.task.maxFailures':
        - 2
'Flume-Output-Settings':
    'Alert-Host':
        - 'localhost:41414'
    'Login-Host':
        - 'localhost:41415'
'CReMa-Config-Location':
    'Path':
        - '/opt/etc/spark/spark.conf'
"""
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
	val appConfig = this.read_config("/opt/etc/spark/sparkConfig.conf")

	//logger.info(appConfig)
	val host = new String(appConfig.get("Streaming-Sources").get("Sources")(0)).split(":")(0)
	val port = new String(appConfig.get("Streaming-Sources").get("Sources")(0)).split(":")(1).toInt
	val readingFreq = appConfig.get("Spark Streaming Settings").get("Read_Interval")(0).toInt
	
	var flumeAlertHost : String = new String(this.appConfig("Flume-Output-Settings")("Alert-Host")(0)).split(":")(0)
	var flumeAlertPort : Int = new String(this.appConfig("Flume-Output-Settings")("Alert-Host")(0)).split(":")(1).toInt
	var flumeLoginHost : String = new String(this.appConfig("Flume-Output-Settings")("Login-Host")(0)).split(":")(0)
	var flumeLoginPort : Int = new String(this.appConfig("Flume-Output-Settings")("Login-Host")(0)).split(":")(1).toInt
	var flumeConnector :FlumeConnector = null
	var flumeLoginConnector: FlumeConnector = null	
	
	def main(args: Array[String]) {

	val csl = new CertLogProcessor("csl",appConfig.get("Filters").get("csl"),appConfig.get("CReMa-Config-Location").get("Path")(0))
	
	val dnim_badips= new CertLogProcessor("dnim-badips",appConfig.get("Filters").get("dnim-badips"),appConfig.get("CReMa-Config-Location").get("Path")(0))
	val urlgrep_analyzer= new CertLogProcessor("urlgrep-analyser",appConfig.get("Filters").get("urlgrep-analyser"),appConfig.get("CReMa-Config-Location").get("Path")(0))
	
	val logins = new LoginProcessor()

	//csl.set_config(csl.read_config(CertLogProcessor.config_path))
	//dnim_badips.set_config(dnim_badips.read_config(CertLogProcessor.config_path))

	val today = Calendar.getInstance().getTime()
	println(today.toString)
	val conf = new SparkConf().setAppName(today.toString)
							  .set("spark.cleaner.ttl",appConfig.get("Spark Streaming Settings").get("spark.cleaner.ttl")(0))
							  .set("spark.task.maxFailures",appConfig.get("Spark Streaming Settings").get("spark.task.maxFailures")(0))
							   .set("spark.localExecution.enabled", "true")
							   .set("spark.streaming.blockInterval", readingFreq.toString)
//						 	   .set("spark.streaming.blockInterval","hdfs://cert-hadoop-001.cern.ch:9000/spark/share/lib/spark-assembly-1.0.0-1.el6.jar")

	val ssc = new StreamingContext(conf,Milliseconds(readingFreq))
	val stream = FlumeUtils.createStream(ssc, host, port,StorageLevel.MEMORY_AND_DISK_2)
	logger.info("Started flume agent on "+host+":"+port)	

	//stream.map(event=>event).foreachRDD(rdd=>saveStub(rdd))

	stream.map(event=> event match{
					case _ if event.event.getHeaders.toString contains("syslog") =>  logins.process(event)
					}).filter(event=> event match{
								case None => false
								case _ => true
					}).foreachRDD(rdd => saveFunction(rdd))


	logger.fatal("receiving data1")
	stream.map(event=> event match{
					case _ if event.event.getHeaders.toString contains("netflow") => dnim_badips.process(event)
					case _ if event.event.getHeaders.toString contains("syslog") =>  csl.process(event)
					case _ if event.event.getHeaders.toString contains "urlgrep"  => urlgrep_analyzer.process(event)
					}).filter(event=> event match{
								case None => false
								case _ => true
					}).foreachRDD(rdd => saveFunction(rdd))
	logger.fatal("receiving data2")
	ssc.start()
	ssc.awaitTermination()
  }
 /* Utility function, merges two maps *
  def mergeConfig(user: Map[String, Map[String,Array[String]]], default: Map[String, Map[String,Array[String]]]) : Map[String, Map[String,Array[String]]] ={
	var foo  = Map[String,Map[String,Array[String]]]()
	val logger = Logger.getLogger("customLogger")
	default.map{
        	tup => if(!(user contains tup._1)){
			  	foo += (tup._1->tup._2)
				//logger.info(tup._1+" wasn't found in user, adding")
			}
	        else if(user.get(tup._1) != tup._2){
	          		foo += (tup._1->user(tup._1))
				//logger.info(tup._1+" user value different than default, updating")
			}
	        else{
	               		foo += (tup._1->(tup._2))
				//logger.info(tup._1+" ,  adding")
			}
	        }
		//logger.info("foo: "+foo)
		return	foo
	}*/

  /*
   *   the configuration from the yaml file which is located at
   *   wherever the config_path variable points to
   */
  def read_config(config_path : String): Map[String, Map[String,Array[String]]] = {
  	var path = config_path	
	val logger = Logger.getLogger("customLogger")
	var defaultconfig :HashMap[String,HashMap[String,ArrayList[String]]] = null
	var finalConfig :HashMap[String,HashMap[String,ArrayList[String]]] = null
	var config :HashMap[String,HashMap[String,ArrayList[String]]] = null
	val yaml = new Yaml()
	try{
        	var cf :String = scala.io.Source.fromFile(path).mkString
        	config = yaml.load(cf).asInstanceOf[java.util.HashMap[String,java.util.HashMap[String,ArrayList[String]]]]
		//val removeBrackets = """  """.r
	}catch{
        	case ex:Exception => 
			println(ex.getMessage)
	    		logger.info("There was a problem opening the provided file, path was "+config_path+" the error was "+ ex.getMessage+" will load the default file")
	        	config = yaml.load(this.defaultConfig).asInstanceOf[java.util.HashMap[String,java.util.HashMap[String,ArrayList[String]]]]
	}
        defaultconfig = yaml.load(this.defaultConfig).asInstanceOf[java.util.HashMap[String,java.util.HashMap[String,ArrayList[String]]]]
	//logger.info("read1: "+defaultconfig)	
	//logger.info("read1: "+config)	
	var scalaConfig = convertConfigToScalaConfig(config)
	var defScalaConfig = convertConfigToScalaConfig(defaultconfig)
	var res = scalaConfig//mergeConfig(scalaConfig,defScalaConfig)
	//logger.info("read_config: " + res)
	res
	}
	def convertConfigToScalaConfig(config:HashMap[String,HashMap[String,ArrayList[String]]]) : Map[String, Map[String,Array[String]]]={
        		val logger = Logger.getLogger("customLogger")
		if(config == null)
		     logger.info("Error, config = null")
	        var scalaConfig = Map[String,Map[String,Array[String]]]()
	        var iter = config.keySet.iterator
	        while(iter.hasNext){
	           var key= iter.next
	           var entry = config.get(key)
	           var iter2 = entry.keySet.iterator
		   var internalMap = Map[String,Array[String]]()
	           while(iter2.hasNext){
	                var key2 = iter2.next
			//logger.info(entry.getClass)
			//logger.info(entry.get(key2).getClass)
	                var entry2 = entry.get(key2)
	                var arr = entry2.toArray.map(el=>el.toString)
			internalMap += (key2->arr)
	           }
	                scalaConfig += (key->internalMap)
	        }
	        //logger.info("convert: "+scalaConfig)
		scalaConfig
	  }
  def saveFunction(rdd: RDD[Option[CERNcertSparkFlumeEvent]]) = {
	val logger = Logger.getLogger("customLogger")
	if(this.flumeConnector == null)
		this.flumeConnector = this.initConnector("alert")
	if(this.flumeLoginConnector == null)
		this.flumeLoginConnector = this.initConnector("login")


	if(rdd.count != 0){
		var data = rdd.collect
		logger.info("after collect, before for")
		for(event<-data){
			if(event.get.event.getHeaders.get("eventType") == "login")
				this.flumeLoginConnector.send(new String(event.get.event.getBody.array), event.get.event.getHeaders)	
			else
				this.flumeConnector.send(new String(event.get.event.getBody.array), event.get.event.getHeaders)	
		}
	}
	
  }
  def saveStub(rdd: RDD[SparkFlumeEvent]) ={
	val logger = Logger.getLogger("customLogger")
	if(this.flumeConnector == null)
		this.flumeConnector = this.initConnector("alert")
	if(this.flumeLoginConnector == null)
		this.flumeLoginConnector = this.initConnector("login")


	if(rdd.count != 0){
		var data = rdd.collect
		logger.info("after collect, before for count = "+rdd.count)

		for(event<-data){
				this.flumeConnector.send(new String(event.event.getBody.array), event.event.getHeaders)	
			logger.info("got data")
		}
	}

  }
  def initConnector(contype : String): FlumeConnector = {
		var flumeHost : String = ""
		var flumePort : Int = -1
		contype match{
			case "alert" =>	flumeHost = flumeAlertHost
					flumePort = flumeAlertPort
			case "login" =>	flumeHost = flumeLoginHost
					flumePort = flumeLoginPort
			case _	     => this.logger.fatal("Trying to initialize the logger with a wrong type, you have a bug")
				}	
		while(true){
			try{
				var connector = new FlumeConnector(flumeHost,flumePort)
				this.logger.info("Succesfully connected to flume agent "+flumeHost + ":"+flumePort)
				return connector

			}catch{
				case e: java.lang.ExceptionInInitializerError => this.logger.fatal("Can't Connect to flume agent, "+flumeHost+":"+flumePort+" please make sure that flume is running")
					Thread.sleep(30)		
				case f:	org.apache.flume.FlumeException =>  this.logger.fatal("Can't Connect to flume agent, "+flumeHost+":"+flumePort+" please make sure that flume is running")
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
