package layers

import akka.actor.{Actor, DeadLetter,ActorPath, ActorRef, Props}
import app._

class MyDeadLetterListner extends Actor {

  def receive = {
    case d: DeadLetter ⇒ {
      var actorIpAddress = d.recipient.path.address.hostPort;

    }
  }
}