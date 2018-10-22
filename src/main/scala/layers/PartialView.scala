package layers

import akka.actor.Actor
import app._


class PartialView extends Actor{

  var activeView : List [String] = List.empty
  var passiveView : List [String] = List.empty
  var ownAddress : String = ""
  val activeSize = 3
  //val ARWL  //Active Random Walk Length
  //val PRWL  //Passive Random Walk Length


  override def receive = {
    case message: Init =>

      ownAddress = message.ownAddress


    }
  }
