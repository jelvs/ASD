package layers.PublishSubscribe

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout

import layers.EpidemicBroadcastTree.MainPlummtree.{BroadCastDeliver, Broadcast}
import layers.MembershipLayer.PartialView.getPeers
import layers.PublishSubscribe.PublishSubscribe.{NeighborSubscription, NeighborUnSubscription, Publish, UnSubscribe}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class PublishSubscribe  extends Actor
{

  val ACTOR_NAME: String = "/user/PublishSubscribe"

  //TODO: Adicionar akka.tcp:// em tudo
  val prependAkkaIdentifier :String = "akka.tcp://"
  var myNeighbors : List[String] = List.empty
  var mySubscriptions : List[String] = List.empty
  var neighborSubscriptions : Map[String, List[String]] = Map.empty
  var ownAddress: String = ""
  val fanout: Integer = 5

  override def receive: Receive = {
    case _: PublishSubscribe.Init =>{

      this.ownAddress = self.path.address.hostPort //returns as node@host:port
      implicit val timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      val future = context.actorSelection("/user/PartialView").resolveOne()
      val partialViewRef: ActorRef = Await.result(future, timeout.duration)
      val future2 = partialViewRef ? getPeers(fanout)
      myNeighbors = Await.result(future2, timeout.duration).asInstanceOf[List[String]]

    }

    case subscribe: PublishSubscribe.Subcribe =>{

      if(!mySubscriptions.contains(subscribe.topic)){
        mySubscriptions = mySubscriptions :+ subscribe.topic
        for(nei <- myNeighbors){
          val actorRef = context.actorSelection(prependAkkaIdentifier.concat(nei.concat(ACTOR_NAME)))
          val neiSub: NeighborSubscription = NeighborSubscription(subscribe.topic, ownAddress)
          actorRef ! neiSub
        }
      }
    }

    case unSubscribe: UnSubscribe =>{

      if(mySubscriptions.contains(unSubscribe.topic)){
        mySubscriptions = mySubscriptions.filter(_ != unSubscribe.topic)
        for(nei <- myNeighbors){
          val actorRef = context.actorSelection(prependAkkaIdentifier.concat(nei.concat(ACTOR_NAME)))
          val neiUnsub: NeighborUnSubscription = NeighborUnSubscription(unSubscribe.topic, ownAddress)
          actorRef ! neiUnsub
        }
      }
    }

    case publish: Publish => {

      val broadcastTree = context.actorSelection("/user/MainPlummtree")
      broadcastTree ! Broadcast(publish)
    }

    case broadCastDeliver: BroadCastDeliver =>{
      //TODO



    }

    case neighborSubscription: NeighborSubscription =>{

      var neiInterested : List[String] = neighborSubscriptions(neighborSubscription.topic)
      if(!neiInterested.contains(neighborSubscription.neighborAddr)){
        neiInterested = neiInterested :+ neighborSubscription.neighborAddr
        neighborSubscriptions(neighborSubscription.topic) -> neiInterested
      }
    }

    case neighborUnSubscription: NeighborUnSubscription =>{

      var neiInterested : List[String] = neighborSubscriptions(neighborUnSubscription.topic)
      if(neiInterested.contains(neighborUnSubscription.neighborAddr)) {
        neiInterested = neiInterested.filter(_ != neighborUnSubscription.neighborAddr)
        neighborSubscriptions(neighborUnSubscription.topic) -> neiInterested
      }
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

  case class DeliverPublish(message: Any)
}


