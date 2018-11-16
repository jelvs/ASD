package app

import akka.actor.{Actor, DeadLetter, Props}

class MyDeadLetterListener extends Actor with akka.actor.ActorLogging {


  def receive = {
    case d: DeadLetter => {
      //log.error(s"DeadLetterMonitorActor : saw dead letter $d")
      //PartialView.NodeFailure(d.recipient.path.address.hostPort)
    }
    case _ => log.info("DeadLetterMonitorActor : got a message")
  }

}

object MyDeadLetterListener{
  val props = Props[MyDeadLetterListener]
}