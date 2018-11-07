package layers

import java.util.concurrent.TimeUnit

import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorNotFound, ActorPath, ActorRef, Props}
import akka.util.Timeout
import app._
import com.typesafe.sslconfig.util.PrintlnLogger
import layers.PartialView.PeerSample

import scala.concurrent.duration.FiniteDuration

class MainPlummtree extends Actor {

  var eagerPushPeers: List[String];
  var lazyPushPeers: List[String];
  var lazyQueues: List[String];
  var missing: List[String];
  var receivedMessages: List[String];
  var partialViewRef: ActorRef;
  val fanout = 4;

  override def receive = {
    case message: Init => {

      implicit val timeout = Timeout(FiniteDuration(1, TimeUnit.SECONDS))
      context.actorSelection("/user/PartialView").resolveOne().onComplete {

        case Success(actorRef) => partialViewRef = actorRef;
        case Failure(actorNotFound) => {
          PrintlnLogger("user/" + "PatialView" + " does not exist")
          //terminate node somehow
        };
      }

      partialViewRef ! getPeers(fanout);

    }

    case peerSample: PeerSample{
    
    }
  }
}

object PartialView {
  val props = Props[PartialView]

  case class PeerSample(peerSample: List[String]);

  case class Broadcast(message: String);

  case class GossipReceive (message :String, messageId: Integer, round: Integer, sender: String);

  case class PruneReceive(sender: String);
}
