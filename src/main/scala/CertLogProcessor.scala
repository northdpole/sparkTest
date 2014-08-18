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
		       		"accept"-> """.*Accepted ([^\s]*)for ([^\s]*).*""".r,
                	"accept_ip"-> """.*Accepted ([^\s]*) for ([^\s]*)(?: from ([^\s]*))?""".r,
	                "fail"-> """.*Failed ([^\s]*) for ([^\s]*).*""".r,
	                "fail_ip"-> """.*Failed ([^\s]*) for ([^\s]*)(?: from ([^\s]*))?""".r,
	                "non_existing_fail" -> """.*Failed ([^\s]*) for invalid user ([^\s]*)""".r,
	                "non_existing_fail_ip"-> """.*Failed ([^\s]*) for invalid user ([^\s]*)(?: from ([^\s]*))?""".r,
	                "logout"-> """.*session closed for user ([^\s]*).*""".r)
	val ssh_commands = Map( "command" -> """.*User ([^\s]*) attempting to execute command (.*) on command line$""".r)
	val windows = Map( "domain_controller"-> Map("login"->Map("loginid"->"""New Logon:.*Logon ID:(0x[0-9a-fA-F]+)""".r,
		                                            	     "usr"->"""New Logon.*Account Name:([a-zA-Z0-9][^\s$]+)""".r,
 				                                     "ip"->"""Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""".r),

			                           "logout" ->Map("usr" -> """User Logoff:[\s]*User Name:([-\.a-zA-Z0-9]+)\$""".r),
		
                			            "failure" -> Map("usr" -> """Account For Which Logon Failed:.*Account Name:([^\s]+)""".r,
		                        			      "ip" -> """Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""".r),

			                            "loginWorkstation"-> Map("workstation" -> """Network Information: Workstation Name:([^\s]+)""".r,
                        			                             "loginid" -> """New Logon:.*Logon ID:(0x[0-9a-fA-F]+)""".r,
                                                			      "usr" -> """New Logon.*Account Name:([a-zA-Z0-9][^\s$]+)""".r,
			                                                      "ip" -> """Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""".r),
	                                             "failureWorkstation" -> Map("workstation" -> """Network Information: Workstation Name:([^\s]+)""".r,
		                                                            	"usr" -> """Account For Which Logon Failed:.*Account Name:([^\s]+)""".r,
       				                                                "ip" -> """"Source Network Address:[^\s]?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)""".r)),
			
	            "terminal_server" -> Map( "login" -> Map ("workstation" -> """Workstation Name:([^\s]+)""".r,
		                                              "loginid" -> """Logon ID:\(0x0,([^)]+)\)""".r,
		                                              "usr" -> """Successful[\s]+[^\s]*[\s]*Logon:[\s]*User Name:([^\s]+)""".r,
		                                              "ip" -> """Source Network Address:([^\s]+)""".r),
		                                "logout" -> Map("usr" -> """User Name:([^\s$]+)""".r),
		                                "failure" -> Map("usr" -> """User Name:([^\s$]+)""".r)
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
        	var hostname = header.get("host")
		var time = header.get("timestamp")
		var syslogtag = header.get("syslogtag")
 		var reg0 = "".r
		var reg1 = "".r
		var matched = false 
		var usr = ""
		var ip = ""
		var misc = ""
		var ret :AvroFlumeEvent = data.get

		//find the correct regex to apply
        	if (header.containsValue("Accepted")){
            		reg0 = CertLogProcessor.ssh_logins.get("accept").get
            		reg1 = CertLogProcessor.ssh_logins.get("accept_ip").get
		}else if (header.containsValue("session closed")){
		        reg0 = CertLogProcessor.ssh_logins.get("logout").get
			val reg0(usr) = new String(ret.getBody.array)
			ret.getHeaders.put("usr",usr)
			return Option(ret)
		}else if (header.containsValue("Failed password")){
			if (header.containsValue("invalid user")){
	                	reg0 = CertLogProcessor.ssh_logins.get("non_existing_fail").get
	        	        reg1 = CertLogProcessor.ssh_logins.get("non_existing_fail_ip").get
  			}else{
        		        reg0 = CertLogProcessor.ssh_logins.get("fail").get
		                reg1 = CertLogProcessor.ssh_logins.get("fail_ip").get
			}
		}else{
	            return None
		}

		matched = new String(ret.getBody.array).matches(reg1.toString)
		
		if(matched == false){
			matched = new String(ret.getBody.array).matches(reg0.toString)
			if(matched == false){
				return None
			}else{
				var reg0(misc,usr) = new String(ret.getBody.array)
				ret.getHeaders.put("usr",usr)
			}
		}else{
			var reg1(misc,usr,ip) = new String(ret.getBody.array)
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
	
        matched = new String(ret.getBody.array).matches(reg.toString)
        if( matched == false){
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
	var regUsr = "".r
	var regIp = "".r
	var regWorkstation = "".r
	var ret = data.get

	// case we have a login
        if ((body contains "An account was successfully logged on") &
            (body contains "Network Information: Workstation Name:") &
            !(body contains "Source Network Address:-") &
             !(body contains "Source Network Address: -" )){
	    regUsr = CertLogProcessor.windows.get("domain_controller").get.get("login").get.get("usr").get
	    regIp = CertLogProcessor.windows.get("domain_controller").get.get("login").get.get("ip").get
        
	// workstation login
	}else if(( body contains "An account was successfully logged on") &
              !(body contains "Source Network Address:-")&
              !(body contains "Source Network Address: -")){
		regWorkstation = CertLogProcessor.windows.get("domain_controller").get.get("loginWorkstation").get.get("workstation").get
		regUsr =  CertLogProcessor.windows.get("domain_controller").get.get("loginWorkstation").get.get("usr").get
		regIp = CertLogProcessor.windows.get("domain_controller").get.get("loginWorkstation").get.get("ip").get
		var matched = new String(body).matches(regWorkstation.toString)
            if(matched == false){return None}
	    val regWorkstation(workstation) = body
            ret.getHeaders.put("workstation",workstation)

        // failed login
	}else if((body contains "An account failed to log on") &
            	(body contains "Network Information: Workstation Name: ") &
	        !(body contains "Source Network Address:-") &
            	!(body contains "Source Network Address: -") &
            	!(body contains "sambatestCS")){

		regUsr = CertLogProcessor.windows.get("domain_controller").get.get("failure").get.get("usr").get
		regIp = CertLogProcessor.windows.get("domain_controller").get.get("failure").get.get("ip").get

        // failed login workstation
	}else if (( body contains "An account failed to log on") &
            	!(body contains "Source Network Address:-") &
            	!(body contains "Source Network Address: -") &
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
	          (body contains "Successful Network Logon")) &
            	!(body contains "Source Network Address:-") &
            	!(body contains "Source Network Address: -")){

        	
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
       
        var regIp(ip) = body
	var regUsr(usr) = body
	ret.getHeaders.put("usr",usr)
	ret.getHeaders.put("ip",ip)
        return Option(ret)
}


   /*
    *    loops throught the items in the config rule and searches
    *    for matches in the interesting parts of the config rule with the
    *    input
    */
    def evaluate_message(config_rule : HashMap[String,ArrayList[String]],message: String): Option[String]  ={
      	var  matched : Int = 0
        var relevant_fields : Int = 0
	val logger = Logger.getLogger("customLogger")
		var keys = config_rule.keySet.iterator
		var key = ""
		var value :java.util.ArrayList[String] = null
 
       	while(keys.hasNext){
			key = keys.next
			if(CertLogProcessor.filters contains key){
				relevant_fields += 1
	  			value = config_rule.get(key)
   	        	        if(value contains message){
                        		matched += 1
				}
        		}
		}
		if( matched >= relevant_fields ){
        		logger.info("Evaluate Message found "+ key.toString)
			return Option("True")
     	}
	logger.info("Evaluate Message Failed to match "+key.toString + "with " +message)
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


        var relevant_config : HashMap[String,HashMap[String,ArrayList[String]]] = new HashMap()
        var iter = config.keySet.iterator
        var i = 0
        while(iter.hasNext){
                var key=iter.next
                var entry = config.get(key).get("sensors")

                if(entry contains "csl"){
                        relevant_config.put(key.toString,config.get(key))
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

/*	if(!(header contains "programname"))
            prog = re.search("""([^\s]*):""".r,values[2])
            if prog is None:
                sys.stderr.write("SyslogBole: loginlog: 246: could not find a programname")
                return
            values[1]["programname"] = prog.group("p")
            values[1]["syslogtag"] = prog.group("p")
*/
        if( !( header containsKey "host") |
            !( header containsKey "timestamp") |
            !( header containsKey "syslogtag")){
		/* TODO: throw incorrect metadata exception */
		return None
	}
	return None

        if(header containsValue "NT"){
            result = this.process_windows(Option(data))}
        else if((header containsValue "sshd") & (body contains  "attemtping to execute command")){
            result = this.process_commands(Option(data))}
        else if((header containsValue "sshd") & !(body contains "attemtping to execute command")){
            result = this.process_ssh(Option(data))}
         result match{
         	case None => return result
         	case _ => {this.insertToDb(result.get)
         				return result}

         }
	}
	
	/* Returns if we found a monitoring event and at what time
	 */
	def benchmark(data: AvroFlumeEvent): Option[String] = {
 
        //var values = this.loginLog(data)

		//return Option("None found")
		
        //check if it"s an event
        var iter = this.config.keySet.iterator
        var i = 0
	val logger = Logger.getLogger("customLogger")
        while(iter.hasNext){
            var key=iter.next
            var entry = config.get(key)
			if(key.toString contains "MONITORING-patterns" ){
				var ret = this.evaluate_message(entry,new String(data.getBody.array))
	            ret  match { 
					case None => 
			                case  _=> ret.get match {case "True"=> logger.info("Benchmark found our alert"); return Option("Found alert")}
	            }
         	}
        }
		return Option("None found")
    }

	
   /* 
    *   if the tup contains any of the values we"re looking for
    *  return the tup
    */
    def process(data: AvroFlumeEvent): Option[AvroFlumeEvent] = {
		if(this.config == null){
			this.config = this.read_config(CertLogProcessor.config_path)
		}
        var values = this.loginLog(data)
        values match{
			case Some(data) => return Option(data)
			case None => }

        //check if it"s an event
	var iter = this.config.keySet.iterator
        var i = 0
	 
	val logger = Logger.getLogger("customLogger")
	if(data.getBody.array.toString contains "google")
		logger.fatal("found alert, should print it")
	else
		logger.fatal("found: " + data.getBody.asCharBuffer.array.toString)
        while(iter.hasNext){
                var key=iter.next
                var entry = config.get(key)
		var ret = this.evaluate_message(entry,new String(data.getBody.array))
		
		ret match{
			case None=> //logger.info("Evaluate_Message returned none for message " + data.getHeaders.toString)
			case _=>ret.get  match {case "True" =>
								logger.info("testing Logger in process, matched: "+ ret)
								data.getHeaders.put("event",key)
								println("CertLogProcessor found event! " + data.getHeaders.toString)
								CertLogProcessor.logger.info("Logger CertLogProcessor found event!" +data.getHeaders.toString); 
								return Option(data)
						case _=>
								logger.info("testing Logger in process, matched: "+ ret)
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
