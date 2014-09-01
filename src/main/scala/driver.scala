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
	Logger.getLogger("customLogger").addAppender(console);
//	println("printing" + logger.toString)
//	logger.debug("Test logging")

/*	 Print the jvm arguments */
//	val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
//	val arguments = runtimeMxBean.getInputArguments();
//	println(arguments.toString)
	
	
	
	val host: String = "localhost"
	val port: Int = 3564
	val readingFreq: Int = 400

	def main(args: Array[String]) {
		
	val processor = new CertLogProcessor()
	processor.set_config(processor.read_config(CertLogProcessor.config_path))
	val today = Calendar.getInstance().getTime();
	println(today.toString)
	val conf = new SparkConf().setAppName(today.toString)
//							.setMaster("local[2]")
							  .set("spark.cleaner.ttl","15")
							  .set("spark.task.maxFailures","2")
	//.setJars("/tmp/cert-log-manager-assembly-1.0.jar")//.set("spark.streaming.unpersist" ,"true")	

	val receiver = new FlumeLogReceiver(processor)
	val ssc = new StreamingContext(conf,Milliseconds(readingFreq))
	val stream = FlumeUtils.createStream(ssc, host, port,StorageLevel.MEMORY_AND_DISK_2)
	stream.map(event=>receiver.printAllMapped(event)).foreachRDD(a=>receiver.printEvent(a))
	ssc.start()
	ssc.awaitTermination()
	
	
	receiver.run()
  }
}
