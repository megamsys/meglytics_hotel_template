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

 //usecase 1

  override def runJob(sc: SparkContext, config: Config) = {

    val dateRDD: RDD[String] = dateBuilder(sc)
    val yearRDD =dateRDD.distinct()
    val finalRDD=yearRDD.zipWithIndex
    val tmpRDD=finalRDD
    finalRDD.foreach(println)
  println(yearRDD)
  //val r =dateRDD.map{case (x, iter) => (x, iter.toList)}
//  println(r)
//  println(count2(dateRDD.toList()))
  println(dateRDD.asInstanceOf[AnyRef].getClass.getSimpleName)
   //val newRDD = yearRDD.map{x=> dateRDD
   //}
//tmpRDD.groupBy(l => l).map(t => (t._1, t._2.length))

    //dateRDD.foreach(println)
  //val newRDD = yearRDD.filter { case (y) => y.endsWith("4")}
  //val arrayList = scala.collection.mutable.MutableList[String]()
//  val newRDD = dateRDD.map(x => x)
  //val countMap = (dateRDD zip yearRDD) groupBy (identity) mapValues {_.length}
  //val arrayList += finalRDD.zip(yearRDD).filter(x => x._1 != x._2).size
   //finalRDD foreach{
     //case(key) =>
    //println("*********")
    //println(key)
    //println(key._1)
//val arrayList += dateRDD.count(_ == key._1)
  //val arrayList += dateRDD.groupBy(identity).mapValues(_.size)

  //}
  //println(arrayList)
   //newRDD.foreach(println)
    //println(newRDD.count())
    //val newRDD = dateRDD filter (a => )
    //val regexpr = """[2000-2020]+""".r
  //val newRDD = dateRDD.map { (y) => y}
  //val newRDD = dateRDD.filter(row => row.contains("4")).count()
   //val result= newRDD.count()

   //println(result)
   //val yearList = scala.collection.mutable.MutableList[String]()
   //val regexpr = """(/d/d/d/d)""".r
  // val newRDD = dateRDD.map(_.)
    //if (yearList != newRDD){
     //yearList += newRDD
     //println("*******************")
     //println(newRDD)
    //}


  }
  def count2(letters: List[String]): Map[String, Int] = {
var map = Map[String, Int]()
for(letter <- letters) map += Pair(letter, map.getOrElse(letter, 0) + 1)
map
}
}
