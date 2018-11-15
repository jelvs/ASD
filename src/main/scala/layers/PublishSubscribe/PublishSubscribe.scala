package layers.PublishSubscribe

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import layers.EpidemicBroadcastTree.MainPlummtree.{BroadCastDeliver, Broadcast}
import layers.MembershipLayer.PartialView.getPeers
import layers.PublishSubscribe.PublishSubscribe._

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class PublishSubscribe  extends Actor
{

  val ACTOR_NAME: String = "/user/PublishSubscribe"
  val AKKA_IP_PREPEND  = "akka.tcp://"
  val PLUM_TREE_ACTOR_NAME: String = "/user/MainPlummtree"
  val PARTIAL_VIEW_ACTOR_NAME = "/user/PartialView"

  var myNeighbors : List[String] = List.empty
  var mySubscriptions : List[String] = List.empty
  var neighborSubscriptions : Map[String, List[String]] = Map.empty
  var ownAddress: String = ""
  val fanout: Integer = 5

  override def receive: Receive = {
    case init: PublishSubscribe.Init =>

      this.ownAddress = init.ownAddress //returns as node@host:port
      implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      val future = context.actorSelection(PARTIAL_VIEW_ACTOR_NAME).resolveOne()
      val partialViewRef: ActorRef = Await.result(future, timeout.duration)
      val future2 = partialViewRef ? getPeers(fanout)
      myNeighbors = Await.result(future2, timeout.duration).asInstanceOf[List[String]]

    case subscribe: PublishSubscribe.Subscribe =>
      if(!mySubscriptions.contains(subscribe.topic)){
        printf("Vou subscrever ao topico: " + subscribe.topic + "\n")
        mySubscriptions = mySubscriptions :+ subscribe.topic
        for(nei <- myNeighbors){
          val actorRef = context.actorSelection(AKKA_IP_PREPEND.concat(nei.concat(ACTOR_NAME)))
          val nei_Subscription: NeighborSubscription = NeighborSubscription(subscribe.topic, ownAddress)
          actorRef ! nei_Subscription
        }
      }

    case unSubscribe: UnSubscribe =>

      if(mySubscriptions.contains(unSubscribe.topic)){
        printf("Vou remover a minha subscrição em: " + unSubscribe.topic)
        mySubscriptions = mySubscriptions.filter(_ != unSubscribe.topic)
        for(nei <- myNeighbors){
          val actorRef = context.actorSelection(AKKA_IP_PREPEND.concat(nei.concat(ACTOR_NAME)))
          val neiUnsubscribe: NeighborUnSubscription = NeighborUnSubscription(unSubscribe.topic, ownAddress)
          actorRef ! neiUnsubscribe
        }
      }

    case publish: Publish =>

      val broadcastTree = context.actorSelection(PLUM_TREE_ACTOR_NAME)
      broadcastTree ! Broadcast(publish)

    case broadCastDeliver: BroadCastDeliver =>
      val publication = broadCastDeliver.asInstanceOf[Publish]
      val interested_Neighbors = neighborSubscriptions(publication.topic)

      printf("Recebi msg de topico " + publication.topic + " para ir " + publication.message + "\n")

      if(mySubscriptions.contains(publication.topic)) {
        //pubsubDeliver(publication.topic)
      }
      for(neighbor <- interested_Neighbors){
        val neiActor = context.actorSelection(AKKA_IP_PREPEND.concat(neighbor.concat(ACTOR_NAME)))
        neiActor ! BroadCastDeliver(broadCastDeliver)
      }



    case neighborSubscription: NeighborSubscription =>
      printf("O meu vizinho " + neighborSubscription.neighborAddr +" esta a subscrever " + neighborSubscription.topic+ "\n")
      var neiInterested : List[String] = neighborSubscriptions(neighborSubscription.topic)
      if(!neiInterested.contains(neighborSubscription.neighborAddr)){
        neiInterested = neiInterested :+ neighborSubscription.neighborAddr
        neighborSubscriptions(neighborSubscription.topic) -> neiInterested
      }

    case neighborUnSubscription: NeighborUnSubscription =>
      printf("O meu vizinho " + neighborUnSubscription.neighborAddr +" esta a dessubscrever " + neighborUnSubscription.topic+ "\n")
      var neiInterested : List[String] = neighborSubscriptions(neighborUnSubscription.topic)
      if(neiInterested.contains(neighborUnSubscription.neighborAddr)) {
        neiInterested = neiInterested.filter(_ != neighborUnSubscription.neighborAddr)
        neighborSubscriptions(neighborUnSubscription.topic) -> neiInterested
      }
  }
}

object PublishSubscribe {
  val props: Props = Props[PublishSubscribe]

  case class Init(ownAddress: String)

  case class Subscribe(topic: String)

  case class UnSubscribe(topic: String)

  case class Publish(topic: String, message: Any )

  case class NeighborSubscription(topic: String, neighborAddr: String)

  case class NeighborUnSubscription(topic: String, neighborAddr: String)

  case class ShortcutDeliver(topic: String )

  case class DeliverPublish(message: Any)

  case class LazyPublish(messageId : Int)

}


