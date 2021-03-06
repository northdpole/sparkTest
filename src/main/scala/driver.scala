import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
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

object Driver {

	/* setup logging */
	val console = new ConsoleAppender(); //create appender
	//configure the appender
	val PATTERN = "%d [%p|%c|%C{1}] %m%n";
	console.setLayout(new PatternLayout(PATTERN)); 
	console.setThreshold(Level.DEBUG);
	console.activateOptions();
	//add appender to any Logger (here is root)
	 var logger = Logger.getLogger("customLogger")
	logger.debug("Test logging")
	Logger.getLogger("customLogger").addAppender(console);
	println("printing" + logger.toString)
	
	/* Print the jvm arguments *
	val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
	val arguments = runtimeMxBean.getInputArguments();
	println(arguments.toString)
*/	
	
	
	val flumeServerName: String = "cert-hadoop-002.cern.ch"
	val port: Int = 3564
	val readingFreq: Int = 400

	def main(args: Array[String]) {
		
	val processor = new CertLogProcessor()
	processor.set_config(processor.read_config(CertLogProcessor.config_path))
	val today = Calendar.getInstance().getTime();
	println(today.toString)
	val conf = new SparkConf().setAppName(today.toString)
							//  .setMaster("yarn-client")
							  .set("spark.cleaner.ttl","8")
							  .set("spark.task.maxFailures","2")
							  //.set("spark.executor.memory","1g")
	//.setJars("/tmp/cert-log-manager-assembly-1.0.jar")//.set("spark.streaming.unpersist" ,"true")	

	val receiver = new FlumeLogReceiver(conf,this.readingFreq,processor,this.flumeServerName,this.port)

	
	receiver.run()
  }
}
