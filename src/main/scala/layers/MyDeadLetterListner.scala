package layers

import akka.actor.{Actor, DeadLetter}

class MyDeadLetterListner extends Actor with akka.actor.ActorLogging {


  def receive = {
    case d: DeadLetter => {
      log.error(s"DeadLetterMonitorActor : saw dead letter $d")
    }
    case _ => log.info("DeadLetterMonitorActor : got a message")
  }

}