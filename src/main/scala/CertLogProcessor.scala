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

object CertLogProcessor{
	var config_path = "/opt/etc/spark/syslog.conf"
	val filters = Array("ips","domains","accounts","programs","patterns")
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
    def evaluate_message(config_rule : HashMap[String,ArrayList[String]],message: String): String  ={
      	var  matched = 0
        var relevant_fields = 0
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
	if(matched >= relevant_fields){
            return true
     }
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
            return this.process_windows(Option(data))}
        else if((header containsValue "sshd") & (body contains  "attemtping to execute command")){
            return this.process_commands(Option(data))}
        else if((header containsValue "sshd") & !(body contains "attemtping to execute command")){
            return this.process_ssh(Option(data))}

	return None
	}
	
	/* Returns if we found a monitoring event and at what time
	 */
	def benchmark(data: AvroFlumeEvent): Option[String] = {
 
        //var values = this.loginLog(data)

		//return Option("None found")
		
        //check if it"s an event
        var iter = this.config.keySet.iterator
        var i = 0
        while(iter.hasNext){
            var key=iter.next
            var entry = config.get(key)
			if(key.toString contains "MONITORING-patterns" ){
				var ret = this.evaluate_message(entry,new String(data.getBody.array))
	            ret  match { 
					case "True" => return Option("Found @: " +  Calendar.getInstance().get(Calendar.YEAR))
	                case  _=> return Option(ret) 
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
        while(iter.hasNext){
                var key=iter.next
                var entry = config.get(key)
		var ret = this.evaluate_message(entry,new String(data.getBody.array))
		ret  match {
			case true =>
				data.getHeaders.put("event",key)
				return Option(data)
			case false => 		}
               
        }
	return None	
    }
}
