package app

import akka.actor.{Actor, Props}
import layers.PublishSubscribe.PublishSubscribe.{DeliverPublish, Publish, Subscribe, UnSubscribe}
import app.Tester.{sendPub, sendSub, sendUnsub}
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
    printf("Vou subscrever esta merda: " + toSub + "\n")
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
    val msg = Random.shuffle(topics).head
    printf("Vou publicar esta merda: " + msg + " neste topico de merda " + topic +"\n")
    subActor ! Publish(topic, msg)
  }

  override def receive: Receive = {

    case init :  Tester.Init =>
      var i : Int = 1
      while(i<=30) {
        i += 1
        topics = topics :+ ("topics" + i)
        messages = messages :+ ("vai pro crlh " + i +" vezes")
      }

    // context.system.scheduler.schedule(180 seconds, 15 seconds)(subscribeShit())
    // context.system.scheduler.schedule(200 seconds, 15 seconds)(publishShit())


    case sendSub : sendSub =>
      subscribeShit()

    case sendUnSub : sendUnsub =>
      unSubShit()

    case publish: sendPub =>
      publishShit()


    case pubdeliver : DeliverPublish =>
      println("Mensagem : " +pubdeliver.message)

  }

}

object Tester{
  var props: Props = Props[Tester]

  case class Init()

  case class sendSub()

  case class sendUnsub()

  case class sendPub()

}