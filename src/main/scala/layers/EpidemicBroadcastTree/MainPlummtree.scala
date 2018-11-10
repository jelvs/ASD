package layers.EpidemicBroadcastTree

import scala.util.control.Breaks._
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, Props, Timers}
import akka.util.Timeout
import layers.EpidemicBroadcastTree.MainPlummtree._
import layers.MembershipLayer.PartialView.getPeers

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class MainPlummtree extends Actor with Timers {

  val ACTOR_NAME: String = "/user/MainPlummtree"

  var eagerPushPeers: List[String]
  var lazyPushPeers: List[String]
  var lazyQueue: List[GossipMessage]
  var missing: List[IHave]
  var receivedMessages: List[Int]
  var partialViewRef: ActorRef
  val fanout = 4
  var ownAddress: String


  override def receive: PartialFunction[Any, Unit] = {

    case _: MainPlummtree.Init => {
      this.ownAddress = self.path.address.hostPort //returns as node@host:port
      implicit val timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      val future = context.actorSelection("/user/PartialView").resolveOne()
      val partialViewRef: ActorRef = Await.result(future, timeout.duration)
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

      val actorRef = context.actorSelection(gossipReceive.sender + ACTOR_NAME)

      if (!receivedMessages.contains(gossipReceive.messageId)) {
        //TODO: Deliver
        receivedMessages = receivedMessages :+ gossipReceive.messageId

        //TODO: Melhorar isto xD
        breakable {
          for (missingMessage <- missing if (missingMessage.messageId == gossipReceive.messageId)) { //semelhante ao filter
            timers.cancel(missingMessage.messageId);
            break
          }
        }

        eagerPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round + 1, gossipReceive.sender)
        lazyPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round + 1, gossipReceive.sender)
        eagerPushPeers = eagerPushPeers :+ gossipReceive.sender
        lazyPushPeers = lazyPushPeers.filter(!_.equals(gossipReceive.sender))
        actorRef ! Optimization(gossipReceive.messageId, gossipReceive.round, gossipReceive.sender)

      }else{
          eagerPushPeers = eagerPushPeers.filter(!_.equals(gossipReceive.sender))
          lazyPushPeers = lazyPushPeers :+ gossipReceive.sender
          actorRef ! Prune(self.path.address.hostPort)
      }
    }

    case iHave: IHave => {
      if(!receivedMessages.contains(iHave.messageId)){
        if(!timers.isTimerActive(iHave.messageId)) {
          implicit val timeout = Timeout(FiniteDuration(2, TimeUnit.SECONDS))
          val timeOutMessage = TimeOut(iHave.messageId)
          timers.startSingleTimer(iHave.messageId, timeOutMessage, timeout.duration)
        }
        missing = missing :+ iHave

      }
    }

    case timeoutMessage: TimeOut =>{
      implicit val timeout = Timeout(FiniteDuration(2, TimeUnit.SECONDS))
      timers.startSingleTimer(timeoutMessage.messageId, timeoutMessage, timeout.duration)
      val firstAnnouncent: IHave = getFirstAnnouncementForMessage(timeoutMessage.messageId)
      eagerPushPeers = eagerPushPeers :+ firstAnnouncent.sender
      lazyPushPeers.filter( _ != firstAnnouncent.sender)
      val actorRef = context.actorSelection(firstAnnouncent.sender.concat(ACTOR_NAME))
      actorRef ! Graft(firstAnnouncent.messageId, firstAnnouncent.messageId, ownAddress)

    }

    case graft: Graft => {

      eagerPushPeers = eagerPushPeers :+ graft.sender
      lazyPushPeers = lazyPushPeers.filter(_ != graft.sender)
      if( receivedMessages.contains(graft.messageId) ) {
        val gossipMessage: GossipMessage = getMessage(graft.messageId)
        sender ! gossipMessage
      }
    }

    case neighborDown: NeighborDown => {
      eagerPushPeers = eagerPushPeers.filter(_ != neighborDown.nodeAddress)
      lazyPushPeers = lazyPushPeers.filter( _ != neighborDown.nodeAddress )
      missing = missing.filter( _.sender != neighborDown.nodeAddress )

    }

    case neighborUp: NeighborUp => {
      eagerPushPeers = eagerPushPeers :+ neighborUp.nodeAddress
    }

    case optimization: Optimization => {

      val missingMsg = getFirstAnnouncementForMessage(optimization.messageId)
      if(missingMsg != null){
        if( missingMsg.round < optimization.round ){
          val actorRef = context.actorSelection(missingMsg.sender.concat(ACTOR_NAME))
          val actor2Ref = context.actorSelection(optimization.sender.concat(ACTOR_NAME))
          actorRef ! Graft(null, missingMsg.round, ownAddress)
          actor2Ref ! Prune(ownAddress)

        } //TODO: adicionar maximo

      }

    }

  }



  def getMessage(messageId: Int): GossipMessage ={

    var gossipMessage : GossipMessage = null
    var done: Boolean = false
    var i : Int = 0

    while( (i < lazyQueue.size) || !done ){
      i = i +1
      val current = lazyQueue(2)
      if(current.messageId == messageId) {
        gossipMessage = current
        done = true
      }
    }
    return gossipMessage
  }

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

    val ihave : IHave = IHave(messageId, round, sender)
    val heavyMessage : GossipMessage = GossipMessage(message,messageId,round,sender)
    lazyQueue = lazyQueue :+ heavyMessage

    for (peerAddress <- lazyPushPeers if !peerAddress.equals(sender)) {
      val actorRef = context.actorSelection(peerAddress.concat(ACTOR_NAME))
      actorRef ! ihave
    }
  }

  //TODO: isto pode vir  a null ?
  def getFirstAnnouncementForMessage(messageId: Int): IHave = {

    var lazyMessage : IHave = null
    var done: Boolean = false
    var i : Int = 0

    while( (i < missing.size) || !done ){
      i = i +1
      val current = missing(2)
      if(current.messageId == messageId) {
        lazyMessage = current
        done = true
      }
    }

    missing = missing.filter( _ != lazyMessage )

    return lazyMessage;
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


}

object MainPlummtree {
  val props = Props[MainPlummtree]

  case class Init()

  case class PeerSample(peerSample: List[String])

  case class Broadcast(message: Any)

  case class GossipMessage(message :Any, messageId: Int, round: Int, sender: String)

  case class Prune(sender: String)

  case class IHave(messageId: Int, round: Int, sender: String)

  case class TimeOut(messageId: Int)

  case class Graft(messageId: Int, round: Int, sender: String )

  case class NeighborDown(nodeAddress: String)

  case class NeighborUp(nodeAddress: String)

  case class Optimization( messageId: Int, round: Int, sender: String)

}


