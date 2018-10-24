package layers

import akka.actor.Actor
import app._

import scala.util.Random


class PartialView extends Actor {

  var myself: String = ""
  var activeView: List[String] = List.empty
  var passiveView: List[String] = List.empty
  val activeSize = 3
  val ARWL = 5 //Active Random Walk Length
  val PRWL = 5 //Passive Random Walk Length


  override def receive = {
    case message: Init => {

      if (!message.contactNode.equals("")) {

        //      val contactNode = message.contactNode
        val process = context.actorSelection(s"${message.contactNode}/user/PartialView")

        //        println("Process path: " + process.toString())
        println("Send Join")

        process ! Join(message.ownAddress)

        addNodeActiveView(message.contactNode)

      }

    }

    case join: Join => {
      addNodeActiveView(join.newNodeAddress)
      //      println("Roger That Join")

      activeView.filter(node => !node.equals(join.newNodeAddress)).foreach(node => {
        println(node)
        //      Criar Processo para enviar Forward Join

        val process = context.actorSelection(s"${node}/user/PartialView")

        process ! ForwardJoin(join.newNodeAddress, ARWL, myself)



      })
      println("active View : ")
      activeView.foreach(aView => println("\t" + aView.toString))

    }


    case forwardJoin: ForwardJoin => {
      if (forwardJoin.arwl == 0 || activeView.size == 1) {

        addNodeActiveView(forwardJoin.newNode)

      } else {

        if(forwardJoin.arwl == PRWL){

          addNodePassiveView(forwardJoin.newNode)

        }

        val n : String = Random.shuffle(activeView.filter(n => !n.equals(forwardJoin.senderAddress))).head

        val process = context.actorSelection(s"${n}/user/PartialView")

        process ! ForwardJoin(n, forwardJoin.arwl, forwardJoin.senderAddress)

      }
      println("passive View : ")
      passiveView.foreach(pView => println("\t" + pView.toString))
      println("active View : ")
      activeView.foreach(aView => println("\t" + aView.toString))


    }

    case disconnect: Disconnect => {


    }


  }

  def addNodeActiveView(node: String) = {
    if (!activeView.contains(node) && !node.equals(myself)) {

      activeView = activeView :+ node
//    println("node added to activeView : " + node)
    }
  }

  def dropRandomNodeActiveView() = {


  }

  def addNodePassiveView(node: String) = {
    if (!passiveView.contains(node) && !activeView.contains(node) && !node.equals(myself)) {

      passiveView = passiveView :+ node

    }
  }
}
