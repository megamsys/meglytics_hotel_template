package template.megam


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.Try



trait HotelTemplate extends spark.jobserver.SparkJob with spark.jobserver.NamedRddSupport with HotelRDDBuilder{

  def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

}

object HotelAnalysisResult extends HotelTemplate {

 //usecase 1

  override def runJob(sc: SparkContext, config: Config) = {

    val dateRDD: RDD[String] = dateBuilder(sc)
    //dateRDD.foreach(println)
    val newRDD = dateRDD filter { case (y) => y.endsWith("4")}
   newRDD.foreach(println)
   println(newRDD.count())


  }
}
