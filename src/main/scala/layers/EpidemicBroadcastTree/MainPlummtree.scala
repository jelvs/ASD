package layers.EpidemicBroadcastTree

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, Props, Timers}
import akka.util.Timeout
import layers.EpidemicBroadcastTree.MainPlummtree._
import layers.MembershipLayer.PartialView.getPeers

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class MainPlummtree extends Actor with Timers {

  val ACTOR_NAME: String = "/user/MainPlummtree"

  var eagerPushPeers: List[String]
  var lazyPushPeers: List[String]
  var lazyQueue: List[HeavyLazyMessage]
  var missing: List[LazyMessage]
  var receivedMessages: List[Int]
  var partialViewRef: ActorRef
  val fanout = 4
  var ownAddress: String

  def toByteArray(value: Any): Array[Byte] = {

    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray

  }

  def eagerPush(message: Any, messageId: Int, round: Int, sender: String): Unit = {
    for (peerAddress <- eagerPushPeers if !peerAddress.equals(sender)) {
      val actorRef = context.actorSelection(peerAddress.concat(ACTOR_NAME))
      actorRef ! GossipMessage(message, messageId, round, sender)
    }
  }

  def lazyPush(message: Any, messageId: Int, round: Int, sender: String): Unit = {

    val lazyMessage : LazyMessage = LazyMessage(messageId, round, sender)
    val heavyLazyMessage: HeavyLazyMessage = HeavyLazyMessage(lazyMessage, message)
    lazyQueue = lazyQueue :+ heavyLazyMessage

    for (peerAddress <- lazyPushPeers if !peerAddress.equals(sender)) {
     val actorRef = context.actorSelection(peerAddress.concat(ACTOR_NAME))
     actorRef ! IHave(messageId,round,sender)
    }
  }

  /*
  def dispatch() = {

    for(peerAddress <- lazyPushPeers) {
      for (lazymessageId <- lazyQueue) {
        val actorRef = context.actorSelection(peerAddress.concat(ACTOR_NAME))
        actorRef ! IHave(lazymessageId, round, sender)
      }
    }
  }

  */

  override def receive: PartialFunction[Any, Unit] = {

    case _: MainPlummtree.Init => {
      this.ownAddress = self.path.address.hostPort
      implicit val timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      var future = context.actorSelection("/user/PartialView").resolveOne()
      val partialViewRef: ActorRef = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
      val future2 = partialViewRef ? getPeers(fanout)
      eagerPushPeers = Await.result(future2, timeout.duration).asInstanceOf[List[String]]
    }

    case broadCast: Broadcast => {
      val messageBytes = toByteArray(broadCast.message)
      val totalMessageBytes = messageBytes ++ ownAddress.getBytes
      val messageId = scala.util.hashing.MurmurHash3.bytesHash(totalMessageBytes)
      eagerPush(broadCast.message, messageId, 0, ownAddress)
      lazyPush(broadCast.message, messageId, 0, ownAddress)
      //TODO: Deliver
      receivedMessages = receivedMessages :+ messageId
    }

    case gossipReceive: GossipMessage => {

      val actorRef = context.actorSelection(gossipReceive.sender +  "/user/TreeRepair")

      if (!receivedMessages.contains(gossipReceive.messageId)) {
        //TODO: Deliver
        receivedMessages = receivedMessages :+ gossipReceive.messageId
        for (missingMessage <- missing if (missingMessage == gossipReceive.messageId)) {
          //TODO: Cancel timer
        }
        eagerPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round +1, gossipReceive.sender)
        lazyPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round +1, gossipReceive.sender)
        eagerPushPeers = eagerPushPeers :+ gossipReceive.sender
        lazyPushPeers = lazyPushPeers.filter( ! _.equals(gossipReceive.sender) )
        actorRef ! Optimization(gossipReceive.messageId, gossipReceive.round, gossipReceive.sender)

      }else {

        eagerPushPeers = eagerPushPeers.filter(! _.equals(gossipReceive.sender))
        lazyPushPeers = lazyPushPeers :+ gossipReceive.sender
        actorRef ! Prune(self.path.address.hostPort)

      }

    }

    case iHave: IHave => {
      if(!receivedMessages.contains(iHave.messageId)){
        if(timers.isTimerActive(iHave.messageId)) {
          implicit val timeout = Timeout(FiniteDuration(2, TimeUnit.SECONDS))
          timers.startSingleTimer(iHave.messageId, TimeOut, timeout.duration
        }
        val newMissingMessage = LazyMessage(iHave.messageId, iHave.round, iHave.sender)
        missing = missing :+ newMissingMessage
      }
    }

    case _: TimeOut


  }
}

object MainPlummtree {
  val props = Props[MainPlummtree]

  case class Init()

  case class PeerSample(peerSample: List[String])

  case class Broadcast(message: Any)

  case class GossipMessage(message :Any, messageId: Int, round: Int, sender: String)

  case class Prune(sender: String)

  case class IHave(messageId: Integer, round: Integer, sender: String)

  case class TreeRepairTimer(messageId: Integer)

  case class TimeOut()

  case class Graft(messageId: Integer, round: Integer, sender: String )

  case class NeighborDown(nodeAddress: String)

  case class NeighborUp(nodeAddress: String)

  case class Optimization( messageId: Integer, round: Integer, sender: String)



}


