package template.megam


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.Try
import scala.collection.mutable.{ LinkedHashMap, ListBuffer }




trait HotelTemplate extends spark.jobserver.SparkJob with spark.jobserver.NamedRddSupport with HotelRDDBuilder{

  def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

}


object HotelAnalysisResult extends HotelTemplate {


  override def runJob(sc: SparkContext, config: Config) = {
//usecase1

    val csvData: RDD[String] = parseData(sc)
    val dateRDD: RDD[String] = dateBuilder(csvData)
    val yearRDD =dateRDD.distinct()
    val finalRDD=yearRDD.zipWithIndex
    val tmpRDD=finalRDD

    //finalRDD.foreach(println)
  //  println(yearRDD)

//usecase3
val services: scala.collection.Map[String, Long] = servicesPreferred(csvData)
println(services)

println("========================")
services.foreach(println)

//usecase4
val feebback = happyCustomers(csvData)

  }



def count2(letters: List[String]): Map[String, Int] = {
var map = Map[String, Int]()
for(letter <- letters) map += Pair(letter, map.getOrElse(letter, 0) + 1)
map
}
}
