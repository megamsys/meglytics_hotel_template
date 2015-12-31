package template.megam

import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.Try
import scala.collection.mutable.{ LinkedHashMap, ListBuffer }

trait HotelTemplate extends spark.jobserver.SparkJob with spark.jobserver.NamedRddSupport with HotelRDDBuilder {

  def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

}

object HotelAnalysisResult extends HotelTemplate {

  override def runJob(sc: SparkContext, config: Config): Any = {

    val csvData: RDD[String] = parseData(sc)

    //usecase1
    val customer: scala.collection.Map[String, Long] = customercount(csvData)
    println(customer)

    //usecase2
    val rooms: scala.collection.Map[String, Long] = roomPreferred(csvData)

    //usecase3
    val services: scala.collection.Map[String, Long] = servicesPreferred(csvData)

    //usecase4
    val feedback: scala.collection.Map[String, Double] = happyCustomers(csvData)

    val final_data = Map("Customer" -> customer, "RoomsPreferred" -> rooms, "Services" -> services, "Feedback" -> feedback)

    return final_data

  }
}
