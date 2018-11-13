package layers.MembershipLayer

import akka.actor.{Actor, Props, Timers}
import layers.EpidemicBroadcastTree.MainPlummtree

import layers.MembershipLayer.PartialView._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.Random

class PartialView extends Actor with Timers
{
  val AKKA_IP_PREPEND  = "akka.tcp://"
  val SYSTEM_NAME = "node"
  val ACTOR_NAME = "/user/PartialView" //é actor name?
  var ownAddress : String = "" //actor re f
  var activeView: List[String] = List.empty //list of node@host:port
  var passiveView: List[String] = List.empty
  val activeViewThreshold = 4
  val passiveViewThreashold = 35
  val ARWL = 5; //Active Random Walk Length
  val PRWL = 5; //Passive Random Walk Length
  var processesAlive = Map[String, Double]()
  var uAlive = Map[String, Double]()


  override def receive = {


    case message: PartialView.Init => {

        val remoteProcess = context.actorSelection(AKKA_IP_PREPEND.concat(message.contactNode.concat(ACTOR_NAME)))  //node@host:port/user/PartialView
        this.ownAddress = self.path.address.hostPort
        remoteProcess ! PartialView.Join(message.ownAddress)

        //TODO : Pending ( not sure if its done this way!! )
        context.system.scheduler.schedule(0 seconds, 5 seconds)(initHeartbeat())
        context.system.scheduler.schedule(0 seconds, 5 seconds)(searchFailedProcesses())
    }

    case join: PartialView.Join => {
      addNodeActiveView(join.newNodeAddress)
      activeView.filter(node => !node.equals(join.newNodeAddress)).foreach(node => {
        val remoteProcess = context.actorSelection(AKKA_IP_PREPEND.concat(node.concat(ACTOR_NAME)))
        remoteProcess ! PartialView.ForwardJoin(join.newNodeAddress, ARWL, ownAddress)
      })
    }


    case forwardJoin: PartialView.ForwardJoin => {
      if (forwardJoin.arwl == 0 || activeView.size == 1) {
        addNodeActiveView(forwardJoin.newNode)
      }else{
        if(forwardJoin.arwl == PRWL){
          addNodePassiveView(forwardJoin.newNode)
        }

        val neighborAdress : String = Random.shuffle(activeView.filter(n => !n.equals(forwardJoin.senderAddress))).head
        val neighborMembershipActor = context.actorSelection(AKKA_IP_PREPEND.concat(neighborAdress.concat(ACTOR_NAME)))
        neighborMembershipActor ! PartialView.ForwardJoin(forwardJoin.newNode ,forwardJoin.arwl-1, forwardJoin.senderAddress)
      }
    }

    case disconnect: PartialView.Disconnect => {
      if (activeView.contains(disconnect.disconnectNode)) {
        activeView = activeView.filter(!_.equals(disconnect.disconnectNode))
        addNodePassiveView(disconnect.disconnectNode)
        processesAlive -= disconnect.disconnectNode

      }
    }

    case getPeers: PartialView.getPeers => {
      val split_Value : Int = math.min(getPeers.fanout, activeView.size)
      val peers = activeView.splitAt(split_Value)
      sender ! peers
    }

    case heartbeat: PartialView.Heartbeat => {
      //println("heartbeat from: " + sender.path.address.toString)
      var timer: Double = System.currentTimeMillis()
      if (processesAlive.contains(sender.path.address.toString)) {
        processesAlive += (sender.path.address.toString -> timer)
      }
    }

    case askToPromote() => {
        promoteProcessToActiveView(sender.path.address.toString)
    }

    case uThere: UThere => {
      val timer: Double = System.currentTimeMillis()
      uAlive += ( uThere.n -> timer )

      val process = context.actorSelection(s"${uThere.n}/user/partialView")
      process ! Verify(sender.path.address.toString)
    }


    case verify : Verify => {

      sender ! ImHere (verify.nodeAddress)

    }


    case addNewtoActive: AddNew => {
      addNodeActiveView(sender.path.address.toString)
    }

    case imHere: ImHere => {
      uAlive -= sender.path.address.toString

      val timer: Double = System.currentTimeMillis()
      processesAlive += (sender.path.address.toString -> timer)

      val process = context.actorSelection(s"${imHere.nodeAddress}/user/partialView")
      process ! SendLiveMessage(sender.path.address.toString)

    }

    case sendLiveMessage: SendLiveMessage => {
      val timer: Double = System.currentTimeMillis()
      processesAlive += (sendLiveMessage.n -> timer)
    }






}



  def addNodeActiveView(node: String) = {
    if (!activeView.contains(node) && !node.equals(ownAddress)) {
      if(activeView.size == activeViewThreshold){
        dropRandomNodeActiveView();
      }
      activeView = activeView :+ node
    }
    addAlive(node)
  }

  def addAlive(node: String) = {

    val timer: Double = System.currentTimeMillis()
    processesAlive += (node -> timer)
  }


  def dropRandomNodeActiveView() = {
    val remoteProcessAdress : String = Random.shuffle(activeView).head //gives node@ip:port
    val remoteActor = context.actorSelection(AKKA_IP_PREPEND.concat(remoteProcessAdress.concat(ACTOR_NAME)))
    remoteActor ! PartialView.Disconnect(ownAddress)
    activeView = activeView.filter(!_.equals(remoteProcessAdress))
    addNodePassiveView(remoteProcessAdress)
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

  def initHeartbeat() = {
    for (h <- activeView) {
      var process = context.actorSelection(s"${h}/user/partialView")
      process ! PartialView.Heartbeat()
    }
  }

  def rUAlive(n : String): Unit ={

    processesAlive -= n
    val timer: Double = System.currentTimeMillis()

    for(n <- activeView){
      var process = context.actorSelection(s"${n}/user/partialView")
      process ! UThere(n)
    }
  }

  def askPassiveToPromote(disconnectedNode: String): Unit ={

    val nodePromote = Random.shuffle(passiveView.filter(node => !node.equals(disconnectedNode)
      || !node.equals(ownAddress))).head

    if (nodePromote != null){
      val process = context.actorSelection(s"${nodePromote}/user/partialView")

        process ! askToPromote()

    }

  }


  def promoteProcessToActiveView(newNode: String) = {
    addNodeActiveView(newNode)
    val process = context.actorSelection(s"${newNode}/user/partialView")
    if (!activeView.contains(newNode) || !((newNode).equals(ownAddress)))
      process ! AddNew()


  }

  def permanentFailure(n: String) = {

    processesAlive -= n
    uAlive -= n


    activeView = activeView.filter(!_.equals(n))
    passiveView = passiveView.filter(!_.equals(n))
    //TODO : send message to all saying node has failed permanently

  }

  def searchFailedProcesses() = {

    for ((n, t) <- processesAlive) {

      // 5 seconds heartbeat
      if ((System.currentTimeMillis() - t) >= 5000) {
        rUAlive(n)
      }
    }

    for ((n, t) <- uAlive) {
      // more than 10 seconds
      if ((System.currentTimeMillis() - t) >= 10000) {
        permanentFailure(n)
      }
    }
  }




}

object PartialView{
  val props = Props[PartialView]

  case class Verify(nodeAddress: String)

  case class ImHere(nodeAddress: String)

  case class SendLiveMessage(n: String)

  case class UThere(n : String)

  case class AddNew()

  case class Heartbeat()

  case class Init (ownAddress : String, contactNode : String);

  case class Join (newNodeAddress: String);

  case class ForwardJoin(newNode: String, arwl: Int, senderAddress: String);

  case class Disconnect (disconnectNode: String);

  case class getPeers(fanout: Integer);

  case class askToPromote()
}

