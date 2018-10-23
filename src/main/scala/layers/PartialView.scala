package layers

import akka.actor.Actor
import app._


class PartialView extends Actor {

  var myself: String = ""
  var activeView: List[String] = List.empty
  var passiveView: List[String] = List.empty
  val activeSize = 3
  val ARWL = 5  //Active Random Walk Length
  val PRWL = 5 //Passive Random Walk Length


  override def receive = {
    case message: Init => {

      if(!message.contactNode.equals("")) {

        val contactNode = message.contactNode
        val process = context.actorSelection(s"${contactNode}/user/PartialView")

        println("Process path: " + process.toString())
        println("Send Join")

        process ! Join(message.ownAddress)

      }

    }

    case message : Join => {
      addNodeActiveView(message.newNodeAddress)
      println("Roger That Join")
    }


    case forwardjoin: ForwardJoin => {


    }

    case disconnect: Disconnect => {



    }


  }

  def addNodeActiveView(node : String) = {


    activeView = activeView :+ node
    println("node added to activeView : " + node )
  }

  def dropRandomNodeActiveView() = {


  }

  def AddNodePassiveView(node : String) = {

  }
}
