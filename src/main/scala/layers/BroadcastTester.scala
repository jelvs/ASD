package layers

import akka.actor.Actor
import layers.EpidemicBroadcastTree.MainPlummtree.BroadCastDeliver

class BroadcastTester  extends Actor {
  override def receive: Receive = {

    case bcDeliver : BroadCastDeliver =>
      printf("Recebi a mensagem \n")

  }

}
