package layers.PublishSubscribe

import akka.actor.{Actor, Props}

class PublishSubscribe  extends Actor
{

  var myNeighbors : List[String]
  var mySubscriptions : List[String]
  var neighborSubscriptions : Map[String, List[String]]

  override def receive: Receive = {
    case init: PublishSubscribe.Init =>{



    }

  }
}

object PublishSubscribe {
  val props = Props[PublishSubscribe]

  case class Init()

  case class Subcribe(topic: String)

  case class UnSubscribe(topic: String)

  case class Publish(topic: String, message: Any )

  case class NeighborSubscription(topic: String, neighborAddr: String)

  case class NeighborUnSubscription(topic: String, neighborAddr: String)

  case class ShortcutDeliver(topic: String )

}


