import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.rdd._
import org.apache.flume.source.avro._
import scala.util.matching._
import org.yaml.snakeyaml.Yaml 
import java.util.HashMap
import java.util.ArrayList
import  scala.io.Source._
import scala.collection._
import scala.collection.JavaConverters;
import java.util.Calendar
import java.sql.PreparedStatement
import java.sql.Connection
import java.util.Properties
import java.sql.DriverManager
import org.apache.log4j.Logger
import CERNcertSparkFlumeEvent._

object CertLogProcessor {
//	logger.setLevel(Level.INFO) 
	val filters = Array("ips","domains","accounts","programs","patterns")
}
@serializable
class CertLogProcessor(
	var sensor :String,
	var filters :Array[String],
	var config_path:String
){
	var config :HashMap[String,HashMap[String,ArrayList[String]]] = read_config(this.config_path)

   /*
    *    loops throught the items in the config rule and searches
    *    for matches in the interesting parts of the config rule with the
    *    input
    */
    def evaluate_message(config_rule : HashMap[String,ArrayList[String]],message: String, ruleName:String): Option[String]  ={
      	var  matched : Int = 0
        var relevant_fields : Int = 0
	val logger = Logger.getLogger("customLogger")
	var log = false
	var keys = config_rule.keySet.iterator
	var key = ""
	var value = ""

 	/*if(message contains "nonexisting_url_to_trigger_an_alert"){
		logger.info("found alert, should print it")
		if( ruleName contains "MONITORING-patterns" ){
			log = true
			logger.info("found our alert Print it already")
		}
	//	else
	//		logger.info(config_rule.keySet.toString)
	}*/
	
       	while(keys.hasNext){
			key = keys.next
			if(CertLogProcessor.filters contains key){
				if(log)	logger.info("relevant field: " + key + " its values houild be :" + value)
				relevant_fields += 1
	  			var vals = config_rule.get(key).iterator
				
				while(vals.hasNext){	
   	 				value = vals.next()       	        	
					if(message contains value){
						if(log)logger.info("matched: " + value)
                        			matched += 1
					}else{
						if(log)logger.info("apparently "+message+"does not contain "+ value)
					}
				}
        		}
		}
		if( matched >= relevant_fields ){
        		if(log)logger.fatal("Evaluate Message found "+ key.toString)
			return Option("True")
     	}
//	logger.info("Evaluate Message Failed to match "+key.toString + "with " +message)
    	return None
    	//return "Not matched for: "+key+"=>"+value +"=>"+message+" ------ Relevant_fields: "+relevant_fields +" - "+ matched
    }

    /*
     *   the configuration from the yaml file which is located at
     *   wherever the config_path variable points to
     */
    def read_config(path: String): HashMap[String,HashMap[String,ArrayList[String]]]  = {
        val yaml = new Yaml()
        var cf :String = scala.io.Source.fromFile(this.config_path).mkString
        val config = yaml.load(cf).asInstanceOf[java.util.HashMap[String,java.util.HashMap[String,ArrayList[String]]]]
	//val removeBrackets = """  """.r
	val logger = Logger.getLogger("customLogger")

        var relevant_config : HashMap[String,HashMap[String,ArrayList[String]]] = new HashMap()
        var iter = config.keySet.iterator
        var i = 0
        while(iter.hasNext){
               var key=iter.next
                var entry = config.get(key).get("sensors")

                if(entry contains this.sensor){
                        relevant_config.put(key.toString,config.get(key))
		//	logger.info("Config values are: "+config.get(key).toString)
                }
        }
        return relevant_config
        }
	def set_config(config: HashMap[String,HashMap[String,ArrayList[String]]]){
		this.config =config
	}
	
   /* 
    *   if the tup contains any of the values we"re looking for
    *  return the tup
    */
    def process(ev: SparkFlumeEvent): Option[CERNcertSparkFlumeEvent] = {
	val logger = Logger.getLogger("customLogger")
	val data = ev.event
	 var iter = this.config.keySet.iterator
        while(iter.hasNext){
                var key=iter.next
                var entry = config.get(key)
		var ret = this.evaluate_message(entry,new String(data.getBody.array),key)
		ret match{
			case None=> //logger.info("Evaluate_Message returned none for message " + data.getHeaders.toString)
			case _=>ret.get  match {case "True" =>
								//logger.fatal("testing Logger in process, matched: "+ ret)
								data.getHeaders.put("event",key)
								data.getHeaders.put("eventType","alert")
								//logger.fatal("CertLogProcessor found event! " + data.getHeaders.toString)
								logger.fatal("Logger CertLogProcessor found event!" + key); 
								return Option(CERNcertSparkFlumeEvent.fromAvroFlumeEvent(data))
						case _=>
						}
               
        }}
	return None
    }


}
