package layers.MembershipLayer

import akka.actor.{Actor, Props, Timers}
import app._

import scala.util.Random

class PartialView extends Actor with Timers
{
  val SYSTEM_NAME = "node";
  val ACTOR_NAME = "/user/PartialView"; //é actor name?
  var ownAddress : String = null ; //actor ref
  var activeView: List[String] = List.empty; //list of node@host:port
  var passiveView: List[String] = List.empty;
  val activeViewThreshold = 6;
  val passiveViewThreashold = 35;
  val ARWL = 5; //Active Random Walk Length
  val PRWL = 5; //Passive Random Walk Length

  override def receive = {
    case message: Init => {

        val remoteProcess = context.actorSelection(message.contactNode.concat(ACTOR_NAME));  //node@host:port/user/PartialView
        this.ownAddress = self.path.address.hostPort;
        remoteProcess ! Join(message.ownAddress);
        addNodeActiveView(message.contactNode);
    }

    case join: Join => {
      addNodeActiveView(join.newNodeAddress);
      activeView.filter(node => !node.equals(join.newNodeAddress)).foreach(node => {
        val remoteProcess = context.actorSelection(node.concat(ACTOR_NAME));
        remoteProcess ! ForwardJoin(join.newNodeAddress, ARWL, ownAddress);
      })
    }


    case forwardJoin: ForwardJoin => {

      if (forwardJoin.arwl == 0 || activeView.size == 1) {
        addNodeActiveView(forwardJoin.newNode);
      }else{

        if(forwardJoin.arwl == PRWL){
          addNodePassiveView(forwardJoin.newNode)
        }

        val neighborAdress : String = Random.shuffle(activeView.filter(n => !n.equals(forwardJoin.senderAddress))).head;
        val neighborMembershipActor = context.actorSelection(neighborAdress.concat(ACTOR_NAME));
        neighborMembershipActor ! ForwardJoin(forwardJoin.newNode ,forwardJoin.arwl-1, ownAddress);
      }
    }

    case disconnect: Disconnect => {
      if (activeView.contains(disconnect.disconnectNode)) {
        activeView = activeView.filter(!_.equals(disconnect.disconnectNode));
        addNodePassiveView(disconnect.disconnectNode);
      }
    }

    case nodeFailure : NodeFailure => {
      dropFromActiveView(nodeFailure.nodeAddress)
      promoteProcessToActiveView();
    }
  }

  def addNodeActiveView(node: String) = {
    if (!activeView.contains(node) && !node.equals(ownAddress)) {
      if(activeView.size == activeViewThreshold){
        dropRandomNodeActiveView();
      }
      activeView = activeView :+ node
    }
  }

  def dropRandomNodeActiveView() = {
    val remoteProcessAdress : String = Random.shuffle(activeView).head; //gives node@ip:port
    val remoteActor = context.actorSelection(SYSTEM_NAME.concat(remoteProcessAdress.concat(ACTOR_NAME)));
    remoteActor ! Disconnect(ownAddress);
    activeView = activeView.filter(!_.equals(remoteProcessAdress));
    addNodePassiveView(remoteProcessAdress);
  }

  def addNodePassiveView(nodeAddress: String) = {

    if (!passiveView.contains(nodeAddress) && !activeView.contains(nodeAddress) && !nodeAddress.equals(ownAddress)) {
      if(passiveView.size == passiveViewThreashold) {
        dropRandomNodePassiveView();
      }
      passiveView = passiveView :+ nodeAddress;
      println("node added to passiveView : " + nodeAddress);
    }
  }

  def dropRandomNodePassiveView(): Unit ={

    val remoteProcessAdress : String = Random.shuffle(passiveView).head;
    passiveView = passiveView.filter(!_.equals(remoteProcessAdress));

  }

  def promoteProcessToActiveView(): Unit ={


  }
}

object PartialView{
  val props = Props[PartialView]

  case class Init (ownAddress : String, contactNode : String);

  case class Join (newNodeAddress: String);

  case class ForwardJoin(newNode: String, arwl: Int, senderAddress: String);

  case class Disconnect (disconnectNode: String);

  case class NodeFailure(nodeAddress: String);
}

