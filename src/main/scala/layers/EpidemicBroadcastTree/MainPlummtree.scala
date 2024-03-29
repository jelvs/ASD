package layers.EpidemicBroadcastTree

import scala.util.control.Breaks._
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.{TimeUnit, TimeoutException}

import akka.pattern.ask
import akka.actor.{Actor, Props, Timers}
import akka.util.Timeout
import app.Application.ShowStats
import layers.EpidemicBroadcastTree.MainPlummtree._
import layers.MembershipLayer.PartialView.getPeers
import layers.PublishSubscribe.PublishSubscribe

import scala.concurrent.Await
import scala.concurrent.duration._

class MainPlummtree extends Actor with Timers {

  val AKKA_IP_PREPEND  = "akka.tcp://"
  val ACTOR_NAME: String = "/user/MainPlummtree"
  val PUBLISH_SUBSCRIBE_ACTOR_NAME = "/user/PublishSubscribe"
  val PARTIAL_VIEW_ACTOR_NAME = "/user/PartialView"
  val FANOUT = 4

  var eagerPushPeers: List[String] = List.empty
  var lazyPushPeers: List[String] = List.empty
  var lazyQueue: List[GossipMessage] = List.empty
  var missing: List[IHave] = List.empty
  var receivedMessages: List[Int] = List.empty
  var ownAddress: String = ""



  var MessagesReceived : Int = 0
  var totalMessagesReceived : Int = 0
  var MessagesSent : Int = 0
  var totalMessagesSent : Int = 0




  override def receive: PartialFunction[Any, Unit] = {
    case init: MainPlummtree.Init =>
      printf("A iniciar\n")
      var done : Boolean = false
      var attempt : Int = 0
      ownAddress =  init.ownAddress//returns as node@host:port

      do {
        try {
          attempt+=1
          implicit val timeout: Timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
          val partialViewRef = context.actorSelection(PARTIAL_VIEW_ACTOR_NAME)
          val future2 = partialViewRef ? getPeers(FANOUT)
          eagerPushPeers = Await.result(future2, timeout.duration).asInstanceOf[List[String]]
          /*
          //test print
          if(eagerPushPeers.nonEmpty){
            println("Eager push peers: ")
            eagerPushPeers.foreach(aView => println("\t" + aView.toString))
          }
          //end test print
          */
          done = true

        } catch {
          case _: TimeoutException => println("Foi tudo com o crlh ")
        }
      }while(!done && attempt < 3)
      val pubSub = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      pubSub ! PublishSubscribe.Init(ownAddress)

    case broadCast: Broadcast =>

      val publishSubscribeActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      val messageBytes = toByteArray(broadCast.message)
      val totalMessageBytes = messageBytes ++ ownAddress.getBytes
      val messageId = scala.util.hashing.MurmurHash3.bytesHash(totalMessageBytes)
      printf("Broadcasting: " + messageId + "\n")
      eagerPush(broadCast.message, messageId, 0, ownAddress)
      lazyPush(broadCast.message, messageId, 0, ownAddress)
      publishSubscribeActor ! BroadCastDeliver(broadCast.message, messageId)
      receivedMessages = receivedMessages :+ messageId

    case gossipReceive: GossipMessage =>

      if (!receivedMessages.contains(gossipReceive.messageId)) {
        printf("Recieved Message for the firsrt time: " + gossipReceive.messageId + "\n")
        val publishSubscribeActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
        publishSubscribeActor ! BroadCastDeliver(gossipReceive.message, gossipReceive.messageId)
        receivedMessages = receivedMessages :+ gossipReceive.messageId

        //TODO: Melhorar isto xD
        breakable{
          for (missingMessage <- missing if missingMessage.messageId == gossipReceive.messageId) { //semelhante ao filter
            timers.cancel(missingMessage.messageId)
            break
          }
        }

        eagerPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round + 1, ownAddress)
        lazyPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round + 1, ownAddress)

        if(!eagerPushPeers.contains(gossipReceive.sender)) {
          eagerPushPeers = eagerPushPeers :+ gossipReceive.sender
        }
        lazyPushPeers = lazyPushPeers.filter(!_.equals(gossipReceive.sender))
        //Optimization(gossipReceive.messageId, gossipReceive.round, gossipReceive.sender)

      }else{
          printf("Already Recieved Message : " + gossipReceive.messageId + "this time sent from "+ gossipReceive.sender + " on round "  + gossipReceive.round +"\n")
          val actorRef = context.actorSelection(gossipReceive.sender + ACTOR_NAME)
          eagerPushPeers = eagerPushPeers.filter(!_.equals(gossipReceive.sender))
          if(!lazyPushPeers.contains(gossipReceive.sender)) {
            lazyPushPeers = lazyPushPeers :+ gossipReceive.sender
          }
            actorRef ! Prune(ownAddress)
      }

    case prune: Prune =>
      eagerPushPeers = eagerPushPeers.filterNot( _ != prune.sender )
      if(!lazyPushPeers.contains(prune.sender))
        lazyPushPeers = lazyPushPeers :+ prune.sender


    //TODO: Este timer da piça não deve trabalhar
    case iHave: IHave =>
     // printf("Recieved Ihave from " + iHave.sender + " of message " + iHave.messageId + "\n")
      if(!receivedMessages.contains(iHave.messageId)){
        if(!timers.isTimerActive(iHave.messageId)) {
          val timeOutMessage = TimeOut(iHave.messageId)
          timers.startSingleTimer(iHave.messageId, timeOutMessage, 5.seconds)
        }
        missing = missing :+ iHave
      }

    case timeoutMessage: TimeOut =>
     // printf("Reached Timeout of message " + timeoutMessage.messageId +"\n")
      implicit val timeout : Timeout = Timeout(FiniteDuration(5, TimeUnit.SECONDS))
      timers.startSingleTimer(timeoutMessage.messageId, timeoutMessage, timeout.duration)
      val firstAnnouncent: IHave = getFirstAnnouncementForMessage(timeoutMessage.messageId)
      if(!eagerPushPeers.contains(firstAnnouncent.sender)) {
        eagerPushPeers = eagerPushPeers :+ firstAnnouncent.sender
      }
      lazyPushPeers = lazyPushPeers.filter( _ != firstAnnouncent.sender)
      val actorRef = context.actorSelection(firstAnnouncent.sender.concat(ACTOR_NAME))
      actorRef ! Graft(firstAnnouncent.messageId, firstAnnouncent.messageId, ownAddress)

    case graft: Graft =>

      if(!eagerPushPeers.contains(graft.sender))
        eagerPushPeers = eagerPushPeers :+ graft.sender
      lazyPushPeers = lazyPushPeers.filter(_ != graft.sender)
      if( receivedMessages.contains(graft.messageId) ) {
        val gossipMessage: GossipMessage = getMessage(graft.messageId)
        sender ! gossipMessage
      }

    case neighborDown: NeighborDown =>
      eagerPushPeers = eagerPushPeers.filter(_ != neighborDown.nodeAddress)
      lazyPushPeers = lazyPushPeers.filter( _ != neighborDown.nodeAddress )
      missing = missing.filter( _.sender != neighborDown.nodeAddress )
      val pubsubActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      pubsubActor ! neighborDown
    //  println("Eager push peers after nei down: ")
      eagerPushPeers.foreach(aView => println("\t" + aView.toString))

    case neighborUp: NeighborUp =>
      if(!eagerPushPeers.contains(neighborUp.nodeAddress)) {
        eagerPushPeers = eagerPushPeers :+ neighborUp.nodeAddress
      }
      val pubsubActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
      pubsubActor ! neighborUp

      println("Eager push peers: ")
      eagerPushPeers.foreach(aView => println("\t" + aView.toString))

    case directDeliver: DirectDeliver =>
      if(!receivedMessages.contains(directDeliver.messageId)) {
        receivedMessages = receivedMessages :+ directDeliver.messageId
        val PubSubActor = context.actorSelection(PUBLISH_SUBSCRIBE_ACTOR_NAME)
        PubSubActor ! BroadCastDeliver(directDeliver.message, directDeliver.messageId)
      }

    case Stats => {
      sender ! ShowStats(totalMessagesSent, totalMessagesReceived)
    }
  }


  def getMessage(messageId: Int): GossipMessage ={

    var gossipMessage : GossipMessage = null
    var done: Boolean = false
    var i : Int = 0

    while( (i < lazyQueue.size)  && !done ){
      val current = lazyQueue(i)
      if(current.messageId == messageId) {
        gossipMessage = current
        done = true
      }
      i = i +1
    }
    gossipMessage
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
   //   printf("A mandar eager oush para: " + peerAddress + "\n")
      val actorRef = context.actorSelection(peerAddress.concat(ACTOR_NAME))
      actorRef ! GossipMessage(message, messageId, round, ownAddress)
    }
  }

  def lazyPush(message: Any, messageId: Int, round: Int, sender: String): Unit = {

    val ihave : IHave = IHave(messageId, round, sender)
    val heavyMessage : GossipMessage = GossipMessage(message,messageId,round,sender)
    lazyQueue = lazyQueue :+ heavyMessage
    for (peerAddress <- lazyPushPeers if !peerAddress.equals(sender)) {
     // printf("A mandar lazy push para :" + peerAddress +"\n")
      val actorRef = context.actorSelection(peerAddress.concat(ACTOR_NAME))
      actorRef ! ihave
    }
  }

  //TODO: isto pode vir  a null ?
  def getFirstAnnouncementForMessage(messageId: Int): IHave = {

    var lazyMessage : IHave = null
    var done: Boolean = false
    var i : Int = 0

   // printf("Missing:\n")
   // missing.foreach(aView => println("\t" + aView.toString))

    printf("Message ID: " + messageId +"\n")
      while ((i < missing.size) && !done) {
        val current = missing(i)
      //  printf("Id da corrent: " + current.messageId +"\n")
        if (current.messageId == messageId) {
          lazyMessage = current
          done = true
        }
        i = i + 1
      }
     // printf("Escolhi a msg com id " + lazyMessage.messageId +"\n")
      //missing = missing.filter(_ != lazyMessage)
      lazyMessage

  }

  def Optimization(messageId : Int, round: Int, sender: String) : Unit = {

    val missingMsg = getFirstAnnouncementForMessage(messageId)
    if(missingMsg != null){
      if( missingMsg.round < round ){
        val actorRef = context.actorSelection(missingMsg.sender.concat(ACTOR_NAME))
        val actor2Ref = context.actorSelection(sender.concat(ACTOR_NAME))
        actorRef ! Graft(-1, missingMsg.round, ownAddress)
        actor2Ref ! Prune(ownAddress)
      } //TODO: adicionar m//aximo

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


}

object MainPlummtree {

  val props: Props = Props[MainPlummtree]

  case class Init(ownAddress: String)

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

  case class BroadCastDeliver(message: Any, messageId: Int)

  case class DirectDeliver(message: Any, messageId: Int)

  case class Stats(nodeAddress: String)
}


