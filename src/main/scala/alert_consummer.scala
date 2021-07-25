import org.apache.kafka.clients.consumer.KafkaConsumer
import play.api.libs.functional.syntax.toFunctionalBuilderOps
import play.api.libs.json.{JsError, JsPath, JsResult, JsSuccess, Json, Reads}

import java.util
import java.util.logging.{Level, Logger}


case class Rapport2(id_drone : Int,ville: String ,list_id: List[Int],list_nom: List[String],list_prenom: List[String],timestamp : String, list_positivite : List[Int], battery: Long){}


object alert_consummer extends App {

  import java.util.Properties

  val TOPIC="peaceland"

  val  props = new Properties()
  props.put("bootstrap.servers", "127.0.0.1:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
   props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  implicit val rapport2Reads: Reads[Rapport2] = (
    (JsPath \ "id_drone").read[Int] and
      (JsPath \ "ville").read[String] and
      (JsPath \ "list_id").read[List[Int]] and
      (JsPath \ "list_nom").read[List[String]] and
      (JsPath \ "list_prenom").read[List[String]] and
      (JsPath \ "timestamp").read[String] and
      (JsPath \ "list_positivite").read[List[Int]] and
      (JsPath \ "battery").read[Long]
    )(Rapport2.apply _)



  Logger.getLogger("org").setLevel(Level.WARNING)
  Logger.getLogger("akka").setLevel(Level.WARNING)
  Logger.getLogger("kafka").setLevel(Level.WARNING)

  def run_forever_alert(): Unit = {
    val records=consumer.poll(100)
    records.forEach(record =>{
      val res = record.value().replace("\\","").dropRight(1).substring(1)
      //println(res)
      val jsonval =Json.parse(res)
      val rapport2Result : JsResult[Rapport2] = jsonval.validate[Rapport2]
      rapport2Result match {
        case JsSuccess(value, path) => {
          val list_positivite = value.list_positivite
          val list_nom = value.list_nom
          val list_prenom = value.list_prenom
          val ville = value.ville
          val timestamp = value.timestamp
          val battery = value.battery
          val list_id = value.list_id
          val id_drone = value.id_drone

          val bad_indices = list_positivite.zipWithIndex.collect{
            case (value, index) if value > 90 => index
          }

          if (bad_indices.nonEmpty){
            // val bad_ids = bad_indices.map(rapport2Result.get.list_id)

            val bad_noms = bad_indices.map(list_nom)
            val bad_prenoms = bad_indices.map(list_prenom)
            val bad_positivity = bad_indices.map(list_positivite)
            println("**********************")
            println(" Warning, in ",ville," the following citizens are not happy : ",bad_prenoms zip bad_noms, "their score is : ", bad_positivity, ". Message sent by drone:", id_drone, "at", timestamp)
            println("**********************")
          }


        }
        case _ => println("Une erreur inntaendue est survenue lors de la lecture du rapport")
      }




    }
    )
    run_forever_alert()
  }

  run_forever_alert()
}
