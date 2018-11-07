package layers.EpidemicBroadcastTree

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import app._
import com.typesafe.sslconfig.util.PrintlnLogger
import layers.EpidemicBroadcastTree.MainPlummtree.{Broadcast, GossipMessage, PeerSample, Prune}
import layers.EpidemicBroadcastTree.TreeRepair.Optimization
import layers.MembershipLayer.PartialView

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class MainPlummtree extends Actor {

  val ACTORNAME: String = "/user/MainPlummtree";

  var eagerPushPeers: List[String];
  var lazyPushPeers: List[String];
  var lazyQueues: List[String];
  var missing: List[MissingMessage];
  var receivedMessages: List[String];
  var partialViewRef: ActorRef;
  val fanout = 4;
  var ownAddress: String;

  def eagerPush(message: String, messageId: Int, round: Int, sender: String): Unit = {
    for (peerAddress <- eagerPushPeers if !peerAddress.equals(sender)) {
      val actorRef = context.actorSelection(peerAddress.concat(ACTORNAME));
      actorRef ! GossipMessage(message, messageId, round, sender);
    }
  }

  def dispatch() = {

  }


  def lazyPush(message: String, messageId: Int, i: Int, sender: String): Unit = {
    for (peerAddress <- lazyPushPeers if !peerAddress.equals(sender)) {
      //TODO: what bro?!
      dispatch();
    }
  }

  override def receive = {
    case message: Init => {
      this.ownAddress = self.path.address.hostPort;
      implicit val timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.actorSelection("/user/PartialView").resolveOne().onComplete {

        case Success(actorRef) => {
          partialViewRef = actorRef;
          actorRef ! getPeers(fanout);
          //TODO: A receçao deve ficar aqui ou não?
        };
        case Failure(actorNotFound) => {
          PrintlnLogger("user/" + "PatialView" + " does not exist")
          //TODO: terminate node somehow
        };
      }
    }

    case peerSample: PeerSample => {
      eagerPushPeers = peerSample.peerSample;
    }

    case broadCast: Broadcast => {
      var messageId = scala.util.hashing.MurmurHash3.stringHash(broadCast.message + ownAddress);
      eagerPush(broadCast.message, messageId, 0, ownAddress);
      lazyPush(broadCast.message, messageId, 0, ownAddress);
      //TODO: Deliver
      receivedMessages = receivedMessages :+ broadCast.message;
    }

    case gossipReceive: GossipMessage => {

      var actorRef = context.actorSelection(gossipReceive.sender +  "/user/TreeRepair");

      if (!receivedMessages.contains(gossipReceive.messageId)) {
        //TODO: Deliver
        receivedMessages = receivedMessages :+ gossipReceive.message;
        for (missingMessage <- missing if (missingMessage == gossipReceive.messageId)) {
          //TODO: Cancel timer
        }
        eagerPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round +1, gossipReceive.sender);
        lazyPush(gossipReceive.message, gossipReceive.messageId, gossipReceive.round +1, gossipReceive.sender);
        eagerPushPeers = eagerPushPeers :+ gossipReceive.sender;
        lazyPushPeers = lazyPushPeers.filter( ! _.equals(gossipReceive.sender) );
        actorRef ! Optimization(gossipReceive.messageId, gossipReceive.round, gossipReceive.sender);

      }else {

        eagerPushPeers = eagerPushPeers.filter(! _.equals(gossipReceive.sender));
        lazyPushPeers = lazyPushPeers :+ gossipReceive.sender;
        actorRef ! Prune(self.path.address.hostPort);

      }

    }
  }
}

object MainPlummtree {
  val props = Props[PartialView]

  case class PeerSample(peerSample: List[String]);

  case class Broadcast(message: String);

  case class GossipMessage(message :String, messageId: Integer, round: Integer, sender: String);

  case class Prune(sender: String);

  }

}
