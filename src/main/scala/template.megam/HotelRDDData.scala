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

private def splitter(c: RDD[String]): List[Serializable] = {
  val data = c.map(_.split(",").map(elem => elem.trim))
  val header = new Formatter(data.take(1)(0))
  return List(data, header)
}

  private def getDates(c: RDD[String]): RDD[String] = {

    val data = c.map(_.split(",").map(elem => elem.trim))
    val header = new Formatter(data.take(1)(0))
    val dates = data.filter(line => header(line, "id") != "id")
    .map(row => header(row, "checkin"))
    dates
  }

private def getRooms(c: RDD[String]): scala.collection.Map[String, Long] = {
 val data = c.map(_.split(",").map(elem => elem.trim))
 val header =new Formatter(data.take(1)(0))
 val rooms = data.filter(line => header(line, "id") != "id").map(row => header(row, "room_type"))
 rooms.countByValue
}

private def getServices(c: RDD[String]): scala.collection.Map[String, Long] = {
//returns a map of all list of services
  val data = c.map(_.split(",").map(elem => elem.trim))
  val header = new Formatter(data.take(1)(0))

  val h_services = data.filter(line => header(line, "id") != "id").map(row => header(row, "room_services"))
  h_services.countByValue
}

private def getFeedback(c: RDD[String]): scala.collection.Map[String, Long] = {

  val data = c.map(_.split(",").map(elem => elem.trim))
  val header = new Formatter(data.take(1)(0))

  val feedback = data.filter(line => header(line, "id") != "id").map(row => header(row, "overall_feedback_score"))

(feedback.countByValue) + ("total" -> feedback.count())

}

  def parseData(sc: SparkContext): RDD[String] = {
    val csvData = sc.textFile(path)
    return csvData
  }
  //uc1
  def dateBuilder(d: RDD[String]): RDD[String] = {
    val dates = getDates(d)
    dates.map(Date.eachRow).
      collect {
        case Some(d) => d.year
      }
  }
  //usecase 1

  def customercount(d: RDD[String]): scala.collection.Map[String,Long] = {
    val count = dateBuilder(d)
    val customer = count.countByValue
    return customer
  }
//usecase2
def roomPreferred(d: RDD[String]): scala.collection.Map[String, Long] = {

  val room_preference = getRooms(d)
  return room_preference
}

  //uc3
  def servicesPreferred(d: RDD[String]): scala.collection.Map[String,Long] = {

    val services = getServices(d)
    println(services)
    //println(services.map(x => x/1000 * 100))
    return services
  }

 //uc4
   def happyCustomers(d: RDD[String]): scala.collection.Map[String, Double] = {

     //1 - very poor, Hate this place, 2 - poor, i don think i ll come ever, 3 - ok(might return), 4- loved it,  5 - Hell yeah, I am coming here!

     val feedback = getFeedback(d)
     val total = feedback.get("total")
     val will_not_return = (feedback.get("1") ++ feedback.get("2")).reduceOption(_ + _).map(x => x/(1000).toDouble * 100)
     val will_return = (feedback.get("4") ++ feedback.get("5")).reduceOption(_ + _).map(x => x/(1000).toDouble * 100)
     val might_return = feedback.get("3").map(x => x/(1000).toDouble * 100)


     val customers = Map("total" -> total.get.toDouble, "will_return" -> will_return.get.toDouble, "will_not_return" -> will_not_return.get.toDouble, "might_return" -> might_return.get.toDouble)

     return customers

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
