import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
import java.io._
import java.util.Calendar
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;


object Driver {

	/* Print the jvm arguments */
	val runtimeMxBean = ManagementFactory.getRuntimeMXBean();
	val arguments = runtimeMxBean.getInputArguments();
	println(arguments.toString)

	val flumeServerName: String = "cert-hadoop-002.cern.ch"
	val port: Int = 3564
	val readingFreq: Int = 400

	def main(args: Array[String]) {
		
	val processor = new CertLogProcessor()
	processor.set_config(processor.read_config(CertLogProcessor.config_path))
	val today = Calendar.getInstance().getTime();
	println(today.toString)
	val conf = new SparkConf().setAppName(today.toString)
							  // .setMaster("yarn-client")
							  .set("spark.cleaner.ttl","8")
							  .set("spark.task.maxFailures","2")
							  //.set("spark.executor.memory","1g")
	//.setJars("/tmp/cert-log-manager-assembly-1.0.jar")//.set("spark.streaming.unpersist" ,"true")	

	val receiver = new FlumeLogReceiver(conf,this.readingFreq,processor,this.flumeServerName,this.port)

	
	receiver.run()
  }
}
