package layers.PublishSubscribe

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import akka.util.Timeout
import layers.EpidemicBroadcastTree.MainPlummtree._
import layers.MembershipLayer.PartialView.getPeers
import layers.PublishSubscribe.PublishSubscribe._
import layers.Tester

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
  var neighborSubscriptions : Map[String, List[(String, Long)]] = Map.empty //map of topic -> list of neighbors and the timestamp when they subscribed that topic
  var ownAddress: String = ""
  val fanout: Integer = 5

  override def receive: Receive = {
    case init: PublishSubscribe.Init =>
      printf("A Iniciar Pub Sub.... \n")
      ownAddress = init.ownAddress //returns as node@host:port
    implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      val future = context.actorSelection(PARTIAL_VIEW_ACTOR_NAME).resolveOne()
      val partialViewRef: ActorRef = Await.result(future, timeout.duration)
      val future2 = partialViewRef ? getPeers(fanout)
      myNeighbors = Await.result(future2, timeout.duration).asInstanceOf[List[String]]

      for (neighbor <- myNeighbors) {
        //printf("nei: " + neighbor + "\n")
        val neighborPubSubActor = context.actorSelection(neighbor.concat(ACTOR_NAME))
        neighborPubSubActor ! NeighborSubscriptions(ownAddress)
      }

      val testActor: ActorSelection = context.actorSelection("/user/Tester")
      testActor ! Tester.Init()

    case subscribe: PublishSubscribe.Subscribe =>
      if (!mySubscriptions.contains(subscribe.topic)) {
   //     printf("Vou subscrever ao topico: " + subscribe.topic + "\n")
        mySubscriptions = mySubscriptions :+ subscribe.topic
        for (nei: String <- myNeighbors) {
      //    printf("Vou dizer ao dred " + nei + " que subscrevi ao topico " + subscribe.topic + "\n")
          val actorRef: ActorSelection = context.actorSelection(nei.concat(ACTOR_NAME))
          val nei_Subscription: NeighborSubscription = NeighborSubscription(subscribe.topic, System.currentTimeMillis(), ownAddress)
          actorRef ! nei_Subscription
        }
      }

    case unSubscribe: UnSubscribe =>
      if (mySubscriptions.contains(unSubscribe.topic)) {
    //    printf("Vou remover a minha subscrição em: " + unSubscribe.topic + "\n")
        mySubscriptions = mySubscriptions.filter(_ != unSubscribe.topic)
        for (nei <- myNeighbors) {
          //printf("Vou dizer ao dred " + nei + " que dessubscrevi ao topico " + unSubscribe.topic + "\n")
          val actorRef = context.actorSelection(nei.concat(ACTOR_NAME))
          val neiUnsubscribe: NeighborUnSubscription = NeighborUnSubscription(unSubscribe.topic, System.currentTimeMillis(), ownAddress)
          actorRef ! neiUnsubscribe
        }
      }

    case publish: Publish =>
      val broadcastTree = context.actorSelection(PLUM_TREE_ACTOR_NAME)
      broadcastTree ! Broadcast(publish)

    case broadCastDeliver: BroadCastDeliver =>
      val message : Any = broadCastDeliver.message
      message match {
        case publication: Publish => {
          printf("Recebi msg de topico " + publication.topic + " com conteudo " + publication.message + "\n")
          if (mySubscriptions.contains(publication.topic)) {
            val tester = context.actorSelection("/user/Tester")
            tester ! DeliverPublish(publication.topic, publication.message)
          }

          try {
            val interested_Neighbors = neighborSubscriptions(publication.topic)
            for (neighborTuple <- interested_Neighbors) {
              val neiActor = context.actorSelection(neighborTuple._1.concat(PLUM_TREE_ACTOR_NAME))
              neiActor ! DirectDeliver(broadCastDeliver.message, broadCastDeliver.messageId)
            }
          } catch {
            case _: NoSuchElementException => //Do nothing no nei subscribe this
          }
        }
        case other => printf(other.getClass +"\n")
      }

        // printf("passei o erro\n")




    case neighborSubscription: NeighborSubscription =>
     // printf("O meu vizinho " + neighborSubscription.neighborAddr + " esta a subscrever " + neighborSubscription.topic + "\n")

      var interestedNodes: List[(String, Long)] = null
      try {
        interestedNodes = neighborSubscriptions(neighborSubscription.topic)
        var nodeTuple: (String, Long) = interestedNodes.find(_._1 == neighborSubscription.neighborAddr).get
        if (nodeTuple == null) {
          nodeTuple = (neighborSubscription.neighborAddr, neighborSubscription.timestamp)
          interestedNodes = interestedNodes :+ nodeTuple
          neighborSubscriptions += neighborSubscription.topic -> interestedNodes
        }

      } catch {
        case _: NoSuchElementException => neighborSubscriptions+= neighborSubscription.topic -> List((neighborSubscription.neighborAddr, neighborSubscription.timestamp))
      }


    case neighborUnSubscription: NeighborUnSubscription =>
    //  printf("O meu vizinho " + neighborUnSubscription.neighborAddr + " esta a dessubscrever " + neighborUnSubscription.topic + "\n")
      var interestedNodes: List[(String, Long)] =  null
      try {
        neighborSubscriptions(neighborUnSubscription.topic)
        var nodeTuple: (String, Long) = interestedNodes.find(_._1 == neighborUnSubscription.neighborAddr).get
        if (nodeTuple != null) {
          if (neighborUnSubscription.timestamp > nodeTuple._2) {
            interestedNodes = interestedNodes.filterNot(_ == nodeTuple)
          }
        }
      }catch {
        case _: Exception => //Do nothing noone is subscribed to this topoc
      }

    case neighborSubscriptions: NeighborSubscriptions =>
    //  printf("Recebi pedido dos meus topicos de " + neighborSubscriptions.nodeAddress + "\n")
      val neighborActor = context.actorSelection(neighborSubscriptions.nodeAddress.concat(ACTOR_NAME))
      neighborActor ! NeighborSubscriptionsReply(mySubscriptions, System.currentTimeMillis(), ownAddress)

    case neighborSubscriptionsReply: NeighborSubscriptionsReply =>
     // printf("O dred " + neighborSubscriptionsReply.nodeAddress + " devolveu os intresses\n ")
      for (topic: String <- neighborSubscriptionsReply.subscriptions) {
        var interestedNodes: List[(String, Long)] = null
        try {
          interestedNodes = neighborSubscriptions(topic)
          interestedNodes.find( tuple => tuple._1 == neighborSubscriptionsReply.nodeAddress) match {
            case Some(_) => //No need to do anything
            case None =>
              interestedNodes = interestedNodes :+ (neighborSubscriptionsReply.nodeAddress, neighborSubscriptionsReply.timestamp)
              neighborSubscriptions += topic -> interestedNodes
          }
        }catch {
          case _: Exception => neighborSubscriptions+= topic -> List((neighborSubscriptionsReply.nodeAddress, neighborSubscriptionsReply.timestamp))
        }
      }

    case neighborUp: NeighborUp =>
      myNeighbors = myNeighbors :+ neighborUp.nodeAddress
      val neighborActor = context.actorSelection(neighborUp.nodeAddress.concat(ACTOR_NAME))
      neighborActor ! NeighborSubscriptions(ownAddress)

    case neighborDown: NeighborDown =>
      myNeighbors = myNeighbors.filterNot(_ != neighborDown.nodeAddress )
      for (nodesPertopic : (String, List[(String, Long)]) <- neighborSubscriptions) {
        var auxList : List[(String, Long)] = nodesPertopic._2
        for( node: (String, Long) <- nodesPertopic._2){
           if( node._1 == neighborDown.nodeAddress ){
               auxList = auxList.filterNot( _._1 == node._1 )
           }
        }
        neighborSubscriptions(nodesPertopic._1) -> auxList
      }
  }
}

object PublishSubscribe {
  val props: Props = Props[PublishSubscribe]

  case class Init(ownAddress: String)

  case class Subscribe(topic: String)

  case class UnSubscribe(topic: String)

  case class Publish(topic: String, message: Any )

  case class NeighborSubscription(topic: String, timestamp : Long,neighborAddr: String)

  case class NeighborUnSubscription(topic: String, timestamp: Long, neighborAddr: String)

  case class ShortcutDeliver(topic: String )

  case class DeliverPublish(topic: String, message: Any)

  case class LazyPublish(messageId : Int)

  case class NeighborSubscriptions(nodeAddress: String)

  case class NeighborSubscriptionsReply(subscriptions: List[String], timestamp: Long, nodeAddress: String)

}


