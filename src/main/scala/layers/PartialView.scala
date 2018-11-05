package layers

import akka.actor.{Actor, ActorLogging, Props}
import app._

import scala.util.Random

class PartialView extends Actor
{

  var activeView: List[String] = List.empty
  var passiveView: List[String] = List.empty
  val activeViewThreshold = 6
  val ARWL = 5 //Active Random Walk Length
  val PRWL = 5 //Passive Random Walk Length

  override def receive = {
    case message: Init => {

        val process = context.actorSelection(message.contactNode)
        process ! Join(message.ownAddress)
        addNodeActiveView(message.contactNode)
    }

    case join: Join => {
      addNodeActiveView(join.newNodeAddress)
      activeView.filter(node => !node.equals(join.newNodeAddress)).foreach(node => {
        println(node)
        //Criar Processo para enviar Forward Join
        val process = context.actorSelection(s"${node}/user/PartialView")
        process ! ForwardJoin(join.newNodeAddress, ARWL, myself)
      })
    }


    case forwardJoin: ForwardJoin => {
      if (forwardJoin.arwl == 0 || activeView.size == 1) {

        addNodeActiveView(forwardJoin.newNode)
        val process = context.actorSelection(s"${forwardJoin.newNode}/user/PartialView")
        if (!((forwardJoin.newNode).equals(myself)) || !activeView.contains(forwardJoin.newNode)) {
          process ! Update()
        }

      } else {

        if(forwardJoin.arwl == PRWL){

          addNodePassiveView(forwardJoin.newNode)

        }

        val n : String = Random.shuffle(activeView.filter(n => !n.equals(forwardJoin.senderAddress))).head

        val process = context.actorSelection(s"${n}/user/PartialView")

        process ! ForwardJoin(n, forwardJoin.arwl-1, forwardJoin.senderAddress)

      }

    }

    case disconnect: Disconnect => {
        if(activeView.contains(disconnect.disconnectNode)){
          activeView = activeView.filter(!_.equals(disconnect.disconnectNode))
          addNodePassiveView(disconnect.disconnectNode)

        }

    }

    case updateActiveView: Update => {
      println("Updating Active view, added - " + sender.path.address.toString)
      addNodeActiveView(sender.path.address.toString)
    }


  }

  def addNodeActiveView(node: String) = {
    if (!activeView.contains(node) && !node.equals(myself)) {

      if(activeView.size +1 == activeViewThreshold){
        dropRandomNodeActiveView()
      }
      activeView = activeView :+ node
      println("node added to activeView : " + node)
    }
    println("active View : ")
    activeView.foreach(aView => println("\t" + aView.toString))
  }



  def dropRandomNodeActiveView() = {
    val n : String = Random.shuffle(activeView).head

    val process = context.actorSelection(s"${myself}/user/PartialView")
    process ! Disconnect(n)

    activeView = activeView.filter(!_.equals(n))
    addNodePassiveView(n)

    println(n + "has been Disconnected")




  }



  def addNodePassiveView(node: String) = {

    if (!passiveView.contains(node) && !activeView.contains(node) && !node.equals(myself)) {

      passiveView = passiveView :+ node
      println("node added to passiveView : " + node)

      if(activeView.size >= activeSize){

        val n : String = Random.shuffle(passiveView).head
        passiveView = passiveView.filter(!_.equals(n))
      }

    }
  }

}

object PartialView{
  val props = Props[PartialView]

  case class Init (ownAddress : String, contactNode : String)

  case class Join (newNodeAddress: String)

  case class ForwardJoin(newNode: String, arwl: Int, senderAddress: String)

  case class Disconnect (disconnectNode: String)

  case class Update ()

}

