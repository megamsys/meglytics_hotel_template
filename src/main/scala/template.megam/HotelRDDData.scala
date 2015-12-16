package template.megam

import org.apache.spark.rdd.RDD
import org.apache.spark._
import akka.util.Timeout
import scala.concurrent.duration.Duration

//case class for header
case class Header(
  id: String,
  name: String,
  phone: String,
  address: String,
  origin_country: String,
  room_type: String,
  checkin: String,
  checkout: String,
  room_services: String,
  hotel_services: String,
  cost: String,
  payment_method: String,
  previous_customer: String,
  overall_feedback_score: String)

case class Date(
  month: String,
  day: String,
  year: String)

object Date {

  val Regex = """(\d\d)/(\d\d)/(\d\d\d\d)""".r

  def eachRow(r: String) = r match {
    case Regex(month, day, year) =>
      Some(Date(month, day, year))
    case _ => None
  }
}

trait HotelRDDBuilder {

  //get ceph url from ENV/conf
  val path = "/tmp/hotel_dataset.csv"

  private def getDates(c: RDD[String]): RDD[String] = {

    val data = c.map(_.split(",").map(elem => elem.trim))
    val header = new Formatter(data.take(1)(0))
    val dates = data.filter(line => header(line, "id") != "id")
    .map(row => header(row, "checkin"))
    dates
  }

  def dateBuilder(sc: SparkContext): RDD[String] = {
    val csvData = sc.textFile(path)
    val dates = getDates(csvData)
    dates.map(Date.eachRow).
      collect {
        case Some(d) => d.year
      }
  }

  def getHeaderdata(sc: SparkContext): RDD[Header] = {

    val filedata = sc.textFile(path)
    filedata.map(_.split(',') match {
      case Array(id, name, phone, address, origin_country, room_type, checkin, checkout, roomservices, hotelservices, cost, method_payment, prevous_customer, overall_feedback_score) =>
        Header(id, name, phone, address, origin_country, room_type, checkin, checkout, roomservices, hotelservices, cost, method_payment, prevous_customer, overall_feedback_score)
    })
  }



}

class Formatter(header: Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array: Array[String], key: String): String = array(index(key))
}
