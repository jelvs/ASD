package layers.MembershipLayer

import akka.actor.{Actor, DeadLetter}

class MyDeadLetterListner extends Actor {

  def receive = {
    case d: DeadLetter ⇒ {
      var actorIpAddress = d.recipient.path.address.hostPort;

    }
  }
}