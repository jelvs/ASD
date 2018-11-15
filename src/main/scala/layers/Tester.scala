package layers

import akka.actor.{Actor, Props}
import layers.PublishSubscribe.PublishSubscribe.{DeliverPublish, Publish, Subscribe, UnSubscribe}
import layers.Tester.{sendPub, sendSub, sendUnsub}

import scala.util.Random

class Tester  extends Actor{

  val PUBLISH_SUBSCRIBE_ACTOR_NAME = "/user/PublishSubscribe"
  var topics : List[String] = List.empty
  var messages: List[String] = List.empty

  override def receive: Receive = {

    case init :  Tester.Init =>
      var i : Int = 1
      while(i<=30) {
        i += 1
        topics = topics :+ ("topics" + i)
        messages = messages :+ ("vai pro crlh " + i +" vezes")
      }

    case sendSub : sendSub =>
      val subActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      val toSub = Random.shuffle(topics).head
      subActor ! Subscribe(toSub)

    case sendUnSub : sendUnsub =>
      val subActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      val toSub = Random.shuffle(topics).head
      subActor ! UnSubscribe(toSub)

    case publish: sendPub =>
      val subActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      val topic = Random.shuffle(topics).head
      val msg = Random.shuffle(topics).head
      subActor ! Publish(topic, msg)


    case pubdeliver : DeliverPublish =>
      println("Mensagem : " +pubdeliver.message)

  }

}

object Tester{
  var props: Props = props[Tester]

  case class Init()

  case class sendSub()

  case class sendUnsub()

  case class sendPub()

}