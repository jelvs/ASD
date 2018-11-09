package layers.EpidemicBroadcastTree

import akka.actor.{Actor, Props}

class TreeRepair  extends Actor{

  override def receive: Receive = {

  }
}

object TreeRepair{

  val props = Props[TreeRepair];

  case class IHave(messageId: Integer, round: Integer, sender: String);

  case class TreeRepairTimer(messageId: Integer);

  case class Graft(messageId: Integer, round: Integer, sender: String );

  case class NeighborDown(nodeAddress: String);

  case class NeighborUp(nodeAddress: String);

  case class Optimization( messageId: Integer, round: Integer, sender: String);
}
