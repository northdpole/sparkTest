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
	val logger :Logger = Logger.getRootLogger()
//	logger.setLevel(Level.INFO)
 
	var config_path = "/opt/etc/spark/syslog.conf"
	val filters = Array("ips","domains","accounts","programs","patterns")
	val oracleQueries = Map("unixlogin" -> "insert into SECURITY_LOG_DEV.LOGIN_DATA_WIDE (message_type, host, send_timestamp, syslogtag, local_user, remote_ip, existing_user)values ('session start', :hostname, to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'),  :syslogtag, :usr, :remote_ip, 'Y') ",
                            "uhnixlogout" -> "insert into SECURITY_LOG_DEV.LOGIN_DATA_WIDE (message_type, host, send_timestamp, syslogtag, local_user, existing_user) values ('session end', :hostname, to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'),  :syslogtag, :usr, 'Y')",
                            "failure"->"insert into SECURITY_LOG_DEV.LOGIN_DATA_WIDE (message_type, host, send_timestamp, syslogtag, local_user, remote_ip, existing_user) values ('failed login', :hostname, to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'),  :syslogtag, :usr, :remote_ip, :existing_user)",
                            "winlogin" -> "insert into SECURITY_LOG_DEV.LOGIN_DATA_WIDE (message_type, host, send_timestamp, syslogtag, local_user, remote_ip, existing_user) values ('session start', lower(:hostname), to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'), :syslogtag, lower(:usr), :remote_ip, 'Y')",
							"winLogout"-> "insert into SECURITY_LOG_DEV.LOGIN_DATA_WIDE (message_type, host, send_timestamp, syslogtag, local_user, existing_user) values ('session end', lower(:hostname), to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'),  :syslogtag, lower(:usr), 'Y')",
                            "winFailure" -> "insert into SECURITY_LOG_DEV.LOGIN_DATA_WIDE (message_type, host, send_timestamp, syslogtag, local_user, remote_ip, existing_user) values ('failed login', lower(:hostname), to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM'),  :syslogtag, lower(:usr), :remote_ip, 'Y')",
                        "sshCommand" -> "insert into SECURITY_LOG_DEV.run_commands(command,host,send_timestamp,local_user,syslogtag) values(:cmd, :hostname, to_timestamp_tz(:ts, 'YYYY-MM-DD\"T\"HH24:MI:SS:TZH.TZM'), :usr, :syslogtag)"
                        )
	val ssh_logins = Map(
		       		"accept"-> """.*Accepted ([^\s]*)for ([^\s]*).*""".r.unanchored,
                	"accept_ip"-> """.*Accepted ([^\s]*) for ([^\s]*)(?: from ([^\s]*))?""".r.unanchored,
	                "fail"-> """.*Failed ([^\s]*) for ([^\s]*).*""".r.unanchored,
	                "fail_ip"-> """.*Failed ([^\s]*) for ([^\s]*)(?: from ([^\s]*))?""".r.unanchored,
	                "non_existing_fail" -> """.*Failed ([^\s]*) for invalid user ([^\s]*)""".r.unanchored,
	                "non_existing_fail_ip"-> """.*Failed ([^\s]*) for invalid user ([^\s]*)(?: from ([^\s]*))?""".r.unanchored,
	                "logout"-> """.*session closed for user ([^\s]*).*""".r.unanchored)
	val ssh_commands = Map( "command" -> """.*User ([^\s]*) attempting to execute command (.*) on command line$""".r.unanchored)
	val windows = Map( "domain_controller"-> Map("login"->Map("loginid"->"""New Logon:.*Logon ID:(0x[0-9a-fA-F]+)""".r.unanchored,
		                                            	     "usr"->"""New Logon.*Account Name:([a-zA-Z0-9][^\s$]+)""".r.unanchored,
 				                                     "ip"->"""Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)?""".r.unanchored),

			                           "logout" ->Map("usr" -> """User Logoff:[\s]*User Name:([-\.a-zA-Z0-9]+)\$""".r.unanchored),
		
                			            "failure" -> Map("usr" -> """Account For Which Logon Failed:.*Account Name:([^\s]+)""".r.unanchored,
		                        			      "ip" -> """Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)?""".r.unanchored),

			                            "loginWorkstation"-> Map("workstation" -> """Network Information: Workstation Name:([^\s]+)""".r.unanchored,
                        			                             "loginid" -> """New Logon:.*Logon ID:(0x[0-9a-fA-F]+)""".r.unanchored,
                                                			      "usr" -> """New Logon.*Account Name:([a-zA-Z0-9][^\s$]+)""".r.unanchored,
			                                                      "ip" -> """Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)?""".r.unanchored),
	                                             "failureWorkstation" -> Map("workstation" -> """Network Information: Workstation Name:([^\s]+)""".r.unanchored,
		                                                            	"usr" -> """Account For Which Logon Failed:.*Account Name:([^\s]+)""".r.unanchored,
       				                                                "ip" -> """"Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)?""".r.unanchored)),
			
	            "terminal_server" -> Map( "login" -> Map ("workstation" -> """Workstation Name:([^\s]+)""".r.unanchored,
		                                              "loginid" -> """Logon ID:\(0x0,([^)]+)\)""".r.unanchored,
		                                              "usr" -> """Successful[\s]+[^\s]*[\s]*Logon:[\s]*User Name:([^\s]+)""".r.unanchored,
		                                              "ip" -> """Source Network Address:([^\s]+)?""".r.unanchored),
		                                "logout" -> Map("usr" -> """User Name:([^\s$]+)""".r.unanchored),
		                                "failure" -> Map("usr" -> """User Name:([^\s$]+)""".r.unanchored)
                            )
		)
}
class CertLogProcessor(
){
	var preparedStatements : Map[String, PreparedStatement] = null
	var connection : Connection = null
	var config :HashMap[String,HashMap[String,ArrayList[String]]] = null

	def process_ssh(data: Option[AvroFlumeEvent]):Option[AvroFlumeEvent] = {
	
	    data match{
		case None => return None
		case Some(data) => val i = 1/* do nothing, continue */}

		var header = data.get.getHeaders()
		var body = new String(data.get.getBody.array)
        	var hostname = header.get("host")
		var time = header.get("timestamp")
		var syslogtag = header.get("syslogtag")
 		var reg0 = "".r.unanchored
		var reg1 = "".r.unanchored
		var usr = ""
		var ip = ""
		var misc = ""
		var ret :AvroFlumeEvent = data.get
		val logger = Logger.getLogger("customLogger")

		//find the correct regex to apply
        	if (header.containsValue("Accepted") || (body contains "Accepted")){
			logger.info("found Accepted")
            		reg0 = CertLogProcessor.ssh_logins.get("accept").get
            		reg1 = CertLogProcessor.ssh_logins.get("accept_ip").get
		}else if (header.containsValue("session closed") || (body contains "session closed")){
		        reg0 = CertLogProcessor.ssh_logins.get("logout").get
			val reg0(usr) = new String(ret.getBody.array)
			ret.getHeaders.put("usr",usr)
			logger.info("found session closed "+ret.getHeaders)
			return Option(ret)
		}else if (header.containsValue("Failed password") || (body contains "Failed password")){
		
        		        reg0 = CertLogProcessor.ssh_logins.get("fail").get
				logger.info("found Failed password")
		                reg1 = CertLogProcessor.ssh_logins.get("fail_ip").get

		}else if (header.containsValue("invalid user") || (body contains "invalid user")){
				logger.info("foun invalid user")
	                	reg0 = CertLogProcessor.ssh_logins.get("non_existing_fail").get
	        	        reg1 = CertLogProcessor.ssh_logins.get("non_existing_fail_ip").get
  		}else{
		    logger.info("\""+header.toString+body+ "\" didn't match anything in ssh")
	            return None
		}

		var matched = reg1.findAllMatchIn(body)
		
		if(matched.isEmpty == true){
			logger.info("couldn't match1, "+new String(ret.getBody.array) + " doesn't matchi " + reg1.toString)
			matched = reg0.findAllMatchIn(body)
			if(matched.isEmpty == false){
				logger.info("couldn't match2, "+new String(ret.getBody.array) + " doesn't matchi " + reg0.toString)
				return None
			}else{
				logger.info("matched2 will try to extract data from  "+reg0.toString + body)
				var reg0(misc,usr) = new String(body)
				ret.getHeaders.put("usr",usr)
				logger.info("new header "+ret.getHeaders)
			}
		}else{	
			logger.info("Matched true => "+reg1+" matches "+new String(ret.getBody.array))
			var reg1(misc,usr,ip) = new String(ret.getBody.array)
			logger.info("1")
			ret.getHeaders.put("usr",usr)
			ret.getHeaders.put("ip",ip)
		}

	return Option(ret)
	}


    def process_commands(data: Option[AvroFlumeEvent]):Option[AvroFlumeEvent] = {

    data match{
	case None => return None
	case Some(data) => val i = 1/* do nothing, continue */}

	var header = data.get.getHeaders()
	var hostname = header.get("host")
    	var time = header.get("timestamp")
	var syslogtag = header.get("syslogtag")
	var reg = CertLogProcessor.ssh_commands.get("command").get
	var matched = false
	var ret = data.get
	val logger = Logger.getLogger("customLogger")
        
	matched = new String(ret.getBody.array).matches(reg.toString)
        if( matched == false){
           logger.info(new String(ret.getBody.array) + "didn't match anything in commands") 
	   return None
	}
        
        var reg(usr,command) = new String(ret.getBody.array)
        ret.getHeaders.put("command",command)
	ret.getHeaders.put("usr",usr)
	return Option(ret)
	}

    def process_windows(data: Option[AvroFlumeEvent]):Option[AvroFlumeEvent] = {

    data match{
	case None => return None
	case Some(data) => val i = 1/* do nothing, continue */}

	var header = data.get.getHeaders()
        var hostname = header.get("host")
        var time = header.get("timestamp")
        var syslogtag = header.get("syslogtag")
        var body = new String(data.get.getBody.array)
	var regUsr = "".r.unanchored
	var regIp = "".r.unanchored
	var regWorkstation = "".r.unanchored
	var ret = data.get
	val logger = Logger.getLogger("customLogger")
	
	// case we have a login
        if ((body contains "An account was successfully logged on") &
            (body contains "Network Information: Workstation Name:") //&
            //!(body contains "Source Network Address:-") &
             //!(body contains "Source Network Address: -" )
	   ){
	    regUsr = CertLogProcessor.windows.get("domain_controller").get.get("login").get.get("usr").get
	    regIp = CertLogProcessor.windows.get("domain_controller").get.get("login").get.get("ip").get
	// workstation login
	}else if(( body contains "An account was successfully logged on") /*&
        	      	!(body contains "Source Network Address:-")&
	              !(body contains "Source Network Address: -")*/
		){
		regWorkstation = CertLogProcessor.windows.get("domain_controller").get.get("loginWorkstation").get.get("workstation").get
		regUsr =  CertLogProcessor.windows.get("domain_controller").get.get("loginWorkstation").get.get("usr").get
		regIp = CertLogProcessor.windows.get("domain_controller").get.get("loginWorkstation").get.get("ip").get
		var matched = new String(body).matches(regWorkstation.toString)
            if(matched == false){return None}
	    val regWorkstation(workstation) = body
            ret.getHeaders.put("workstation",workstation)

        // failed login
	}else if((body contains "An account failed to log on") &
            	(body contains "Network Information: Workstation Name: ") &/*
	        !(body contains "Source Network Address:-") &
            	!(body contains "Source Network Address: -") &*/
            	!(body contains "sambatestCS")
		){

		regUsr = CertLogProcessor.windows.get("domain_controller").get.get("failure").get.get("usr").get
		regIp = CertLogProcessor.windows.get("domain_controller").get.get("failure").get.get("ip").get

        // failed login workstation
	}else if (( body contains "An account failed to log on") &/*
            	!(body contains "Source Network Address:-") &
            	!(body contains "Source Network Address: -") &*/
            	!(body contains "sambatestCS")){
		
		regWorkstation = CertLogProcessor.windows.get("domain_controller").get.get("failureWorkstation").get.get("workstation").get
                regUsr =  CertLogProcessor.windows.get("domain_controller").get.get("failureWorkstation").get.get("usr").get
                regIp = CertLogProcessor.windows.get("domain_controller").get.get("failureWorkstation").get.get("ip").get
		  var matched = new String(body).matches(regWorkstation.toString)
            if(matched == false){return None}
            val regWorkstation(workstation) = body
            ret.getHeaders.put("workstation",workstation)

        //terminal server login
	}else if(((body contains "Successful Logon") |
		  (body contains "Successful Network Logon")) /*&
            		!(body contains "Source Network Address:-") &
	            	!(body contains "Source Network Address: -")*/
		){

        	
		regWorkstation = CertLogProcessor.windows.get("terminal_server").get.get("login").get.get("workstation").get
                regUsr =  CertLogProcessor.windows.get("terminal_server").get.get("login").get.get("usr").get
                regIp = CertLogProcessor.windows.get("terminal_server").get.get("login").get.get("ip").get
                var matched = new String(body).matches(regWorkstation.toString)
        	if(matched == false){return None}
	        var regWorkstation(workstation) = body
	        ret.getHeaders.put("workstation",workstation)


	//terminal server logoff
	}else if(body contains "User Logoff"){
	    regUsr =  CertLogProcessor.windows.get("terminal_server").get.get("logout").get.get("usr").get
	    var regUsr(usr) = body
	    ret.getHeaders.put("usr",usr)
	    return Option(ret)
       //terminal server failure
        }else if(body contains "Logon Failure"){
	    regUsr =  CertLogProcessor.windows.get("terminal_server").get.get("failure").get.get("usr").get
            var regUsr(usr) = body
            ret.getHeaders.put("usr",usr)
            return Option(ret)
	}
	var ip = regIp.findFirstMatchIn(body).get.toString 
	var usr =  regUsr.findFirstMatchIn(body).get.toString
	logger.info(body.matches(regIp.toString)) 
	logger.info(body.matches(regUsr.toString))
	ret.getHeaders.put("usr",usr.toString)
	ret.getHeaders.put("ip",ip.toString)
	return Option(ret)
}


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

 	if(message contains "nonexisting_url_to_trigger_an_alert"){
		logger.info("found alert, should print it")
		if( ruleName contains "MONITORING-patterns" ){
			log = true
			logger.info("found our alert Print it already")
		}
	//	else
	//		logger.info(config_rule.keySet.toString)
	}
	
       	while(keys.hasNext){
			key = keys.next
			if(CertLogProcessor.filters contains key){
				if(log)	logger.info("relevant field: " + key + "its values houild be :" + value)
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
        		logger.fatal("Evaluate Message found "+ key.toString)
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

                if(entry contains "csl"){
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
     * checks if the value  is a login line
     */
    def loginLog(data : AvroFlumeEvent ) : Option[AvroFlumeEvent] ={
        var header = data.getHeaders()
        var body = new String(data.getBody.array)
        var result : Option[AvroFlumeEvent] = None
	val logger = Logger.getLogger("customLogger")
	val progName = """([^\s]*):""".r.unanchored
	var notFound = "noTag"

	if(!(header.keySet contains "programname") & !(header.keySet contains "syslogtag")){
           val matched = progName.findFirstMatchIn(body)
            if(matched == None){
                logger.info("Syslog: loginlog: 246: could not find a programname or syslogtag, body is " + body)
	        //header.put("programname",notFound)
		//header.put("syslogtag", notFound)

	   }else{
	        header.put("programname",matched.get.toString)
		header.put("syslogtag", matched.get.toString)
		}
	}
        if( !( header containsKey "host")){
		logger.fatal("didn't find host in header: " + header.toString)
		return None
	} 
        if ( !( header containsKey "timestamp") ){
		logger.fatal("didn't found timestamp in header: " + header.toString)
		return None
	}
         if ( !( header containsKey "syslogtag")){
		/* TODO: throw incorrect metadata exception */
		logger.fatal("didn't find syslogtag in the header "+header.toString)
		return None
	}
        if(header.get("syslogtag") == "NT:"){
            //TODO uncomment result = this.process_windows(Option(data))
	}
        /* TODO add an if else */

	if((header.toString contains "sshd") & (body contains  "attemtping to execute command")){
		logger.info("recognized command login")
            result = this.process_commands(Option(data))
	}
        else{
//		logger.info("not matched: ssd,attempting to execute command:+ "+header.toString + "\n:\n" + body)
	}
	 if((header.toString contains "sshd") & !(body contains "attemtping to execute command")){
		logger.info("recognized ssh login " +header.toString + body)
            result = this.process_ssh(Option(data))
	}
        else{
		//logger.info("not matched: ssd:+ "+header.toString + "\n:\n" + body)
	}
         result match{
         	case None => return None
         	case _ => {
			//todo uncomment this.insertToDb(result.get)
         		return result
		}

         }
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
	
//	logger.fatal(" message header== "+data.getHeaders.toString + "\n message body == " + new String(data.getBody.array) + "\n\n")
        
       	var values = loginLog(data)
	 values match{
			case Some(loginEvent) => logger.fatal("found login")
				    return Option(loginEvent)
			case None => //logger.info("couldn't find login for" +data.getHeaders.toString+new String(data.getBody.array)) 
		    }

        //check if it"s an event
	var iter = this.config.keySet.iterator
//        var i = 0
	 
return None
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

    def init(dbHost : String, dbName: String, dbPort: String, dbUser: String, dbpass: String){
        var url = "jdbc:oracle:thin:"+dbName+"/"+dbpass
    	 
    	 //properties for creating connection to Oracle database
        //var props = new Properties();
        //props.setProperty("password", dbpass);
        this.connection = DriverManager.getConnection(url)
        for(key <- CertLogProcessor.oracleQueries.keys){
        	this.preparedStatements += (key.toString -> this.connection.prepareStatement(CertLogProcessor.oracleQueries.get(key.toString).get))
        }
    }

    def insertToDb(data: AvroFlumeEvent){
    	var header = data.getHeaders()

    	var headerKeys :Array[String] = header.keySet.toArray.asInstanceOf[Array[String]]
    	var oracleKeys = CertLogProcessor.oracleQueries.keys.toArray
    	var query:String = ""

    	for(key <- headerKeys){
    		if(oracleKeys contains key){
    			var stmt = this.preparedStatements.get(key).get
		    	stmt.setString(1,header.get("hostname").toString)
	    		stmt.setString(2,header.get("ts").toString)
    			stmt.setString(3,header.get("syslogtag").toString)
    			stmt.setString(4,header.get("usr").toString)
				if(!(key contains "logout"))
    				stmt.setString(5,header.get("remote_ip").toString)
    		}
		}

   }
}
