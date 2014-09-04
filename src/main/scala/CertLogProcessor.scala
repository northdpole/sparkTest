import org.apache.spark.streaming._
import org.apache.spark.rdd._
import org.apache.flume.source.avro._
import org.apache.spark.streaming.flume._
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

object CertLogProcessor{
//	logger.setLevel(Level.INFO) 
	var config_path = "/opt/etc/spark/syslog.conf"
	val filters = Array("ips","domains","accounts","programs","patterns")
}
@serializable
class CertLogProcessor(
	var sensor :String
){
	var config :HashMap[String,HashMap[String,ArrayList[String]]] = null

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
	var vals :java.util.Iterator[String] = null
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
	  			vals = config_rule.get(key).iterator
				
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
        var cf :String = scala.io.Source.fromFile(CertLogProcessor.config_path).mkString
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
    def process(data: AvroFlumeEvent): Option[AvroFlumeEvent] = {
		if(this.config == null){
			this.config = this.read_config(CertLogProcessor.config_path)
		}

	val logger = Logger.getLogger("customLogger")
	
	//logger.fatal(" message header== "+data.getHeaders.toString + "\n message body == " + new String(data.getBody.array) + "\n\n")
//        if(new String(data.getBody.array) contains "alert")
//		logger.info("alerted")
	 var iter = this.config.keySet.iterator
//        var i = 0
	 
//logger.info("process received data")

	//	else
		//logger.fatal("\n\n\n\n found: " + new String(data.getBody.array))

        while(iter.hasNext){
                var key=iter.next
                var entry = config.get(key)
		var ret = this.evaluate_message(entry,new String(data.getBody.array),key)
		ret match{
			case None=> //logger.info("Evaluate_Message returned none for message " + data.getHeaders.toString)
			case _=>ret.get  match {case "True" =>
								//logger.fatal("testing Logger in process, matched: "+ ret)
								data.getHeaders.put("event",key)
								//logger.fatal("CertLogProcessor found event! " + data.getHeaders.toString)
								logger.fatal("Logger CertLogProcessor found event!" + key); 
								return Option(data)
						case _=>
						}
               
        }}
	return None
    }


}
