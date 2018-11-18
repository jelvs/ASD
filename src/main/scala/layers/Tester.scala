package layers

import akka.actor.{Actor, Props}
import layers.PublishSubscribe.PublishSubscribe.{DeliverPublish, Publish, Subscribe, UnSubscribe}
import layers.Tester.{sendPub, sendSub, sendUnsub}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random
import scala.concurrent.duration._

class Tester  extends Actor{

  val PUBLISH_SUBSCRIBE_ACTOR_NAME = "/user/PublishSubscribe"
  val TESTER_NAME = "/user/Tester"
  var topics : List[String] = List.empty
  var messages: List[String] = List.empty

  def subscribeShit(): Unit = {
    val subActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
    val toSub = Random.shuffle(topics).head
    printf("Vou subscrever o topico: " + toSub + "\n")
    subActor ! Subscribe(toSub)
  }

  def unSubShit(): Unit = {
    val subActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
    val toSub = Random.shuffle(topics).head
    subActor ! UnSubscribe(toSub)
  }

  def publishShit() : Unit = {
    val subActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
    val topic = Random.shuffle(topics).head
    val msg = Random.shuffle(messages).head
    printf("Vou publicar: " + msg + " neste topico " + topic +"\n")
    subActor ! Publish(topic, msg)
  }

  override def receive: Receive = {

    case init :  Tester.Init =>
      printf("A iniciar tester...\n")
      var i : Int = 1
      while(i<=30) {
        i += 1
        topics = topics :+ ("topico" + i)
        messages = messages :+ ("Vamos chumbar a ASD " + i +" vezes")
      }

     context.system.scheduler.schedule(70 seconds, 15 seconds)(subscribeShit())
     context.system.scheduler.schedule(75 seconds, 30 seconds)(publishShit())


    case sendSub : sendSub =>
      subscribeShit()

    case sendUnSub : sendUnsub =>
      unSubShit()

    case publish: sendPub =>
      publishShit()


    case pubdeliver : DeliverPublish =>
      println("Mensagem do topic: "+ pubdeliver.topic +" com contenudo: " +pubdeliver.message + "\n")

  }

}

object Tester{
  
  var props: Props = Props[Tester]


  case class Init()

  case class sendSub()

  case class sendUnsub()

  case class sendPub()

}