package layers.MembershipLayer

import akka.actor.{Actor, Props, Timers}
import layers.EpidemicBroadcastTree.MainPlummtree
import layers.EpidemicBroadcastTree.MainPlummtree.{NeighborDown, NeighborUp}
import layers.MembershipLayer.PartialView._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class PartialView extends Actor with Timers
{
  //val AKKA_IP_PREPEND  = "akka.tcp://"
  val SYSTEM_NAME = "node"
  val ACTOR_NAME = "/user/PartialView"
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

      if (!message.contactNode.equals("")) {

        //val contactNode = message.contactNode
        val process = context.actorSelection(message.contactNode.concat(ACTOR_NAME))

        //println("Process path: " + process.toString())

        process ! Join(message.ownAddress)
        addNodeActiveView(message.contactNode)

        //context.system.scheduler.schedule(0 seconds, 30 seconds)((sendRandomRefreshPassive()))
      }

      context.system.scheduler.schedule(0 seconds, 3 seconds)(initHeartbeat())

      context.system.scheduler.schedule(0 seconds, 3 seconds)((searchFailedProcesses()))

    }


    case join: Join => {

      addNodeActiveView(join.newNodeAddress)

      val process = context.actorSelection(s"${sender.path.address.toString}/user/Plummtree")
      process ! NeighborUp(join.newNodeAddress)

      activeView.filter(node => !node.equals(join.newNodeAddress)).foreach(node => {
        val remoteProcess = context.actorSelection(node.concat(ACTOR_NAME))
        remoteProcess ! ForwardJoin(sender.path.address.toString, ARWL, ownAddress)
      })
    }


    case forwardJoin: ForwardJoin => {
      if (forwardJoin.arwl == 0 || activeView.size == 1) {
        addNodeActiveView(forwardJoin.newNode)

        val process = context.actorSelection("/user/Plummtree") //nao é isto tira o f.newnode
        val process2 = context.actorSelection(s"${forwardJoin.newNode}/user/PartialView")
        process ! NeighborUp(forwardJoin.newNode)
        process2 ! AddNew() //porque nao neighbor up?

      }else{
        if(forwardJoin.arwl == PRWL){
          addNodePassiveView(forwardJoin.newNode)
        }


          val neighborAddress: String = Random.shuffle(activeView.filter(n => !n.equals(sender.path.address.toString)
            && !(n.equals(forwardJoin.newNode)) && !(n.equals(forwardJoin.senderAddress)))).head

          val neighborMembershipActor = context.actorSelection(neighborAddress.concat(ACTOR_NAME))


          neighborMembershipActor ! ForwardJoin(forwardJoin.newNode, forwardJoin.arwl - 1, ownAddress)

        }


    }




    case disconnect: Disconnect => {
      if (activeView.contains(disconnect.disconnectNode)) {
        activeView = activeView.filter(!_.equals(disconnect.disconnectNode))
        addNodePassiveView(disconnect.disconnectNode)

        //processesAlive -= disconnect.disconnectNode

        //askPassiveToPromote(disconnect.disconnectNode) //acho que nao é preciso

      }
    }




    case getPeers: getPeers => {
      val split_Value : Int = math.min(getPeers.fanout, activeView.size)
      val peers = activeView.splitAt(split_Value)
      sender ! peers
    }



    case askToPromote(priority) => {

      if(priority.equals("High")){
          promoteProcessToActiveView(sender.path.address.toString)
      }else{
        if(activeView.size < activeViewThreshold){
          promoteProcessToActiveView(sender.path.address.toString)
        }

      }

    }



    case addNewtoActive: AddNew => {
      addNodeActiveView(sender.path.address.toString)
    }

    case nodeFailure : PartialView.NodeFailure => {
      //activeView = activeView.filter( !_.equals(nodeFailure.nodeAddress))
      permanentFailure(nodeFailure.nodeAddress)
      askPassiveToPromote(nodeFailure.nodeAddress)
    }



    case receiveRefreshSendPassive: ReceiveRefreshSendPassive =>{

      receiveToRefreshSend(sender.path.address.toString, receiveRefreshSendPassive.nodesToRefresh )
    }

    case receiveRefreshPassive: ReceiveRefreshPassive =>{
      receiveToRefreshPassive(sender.path.address.toString, receiveRefreshPassive.nodesToRefresh )

    }



    case uThere: UThere => {
      val timer: Double = System.currentTimeMillis()
      uAlive += ( uThere.n -> timer )

      val process = context.actorSelection(s"${uThere.n}/user/PartialView")
      process ! Verify(sender.path.address.toString)
    }


    case verify : Verify => {

      sender ! ImHere(verify.nodeAddress)

    }

    case imHere: ImHere => {
      uAlive -= sender.path.address.toString

      val timer: Double = System.currentTimeMillis()
      processesAlive += (sender.path.address.toString -> timer)

      val process = context.actorSelection(s"${imHere.nodeAddress}/user/PartialView")
      process ! SendLiveMessage(sender.path.address.toString)

    }

    case sendLiveMessage: SendLiveMessage => {
      val timer: Double = System.currentTimeMillis()
      processesAlive += (sendLiveMessage.n -> timer)
    }




    case heartbeat: Heartbeat => {
      //println("heartbeat from: " + sender.path.address.toString)
      var timer: Double = System.currentTimeMillis()
      if (processesAlive.contains(sender.path.address.toString)) {
        processesAlive += (sender.path.address.toString -> timer)
      }
    }


  }

  /*case sendRandomRefreshPassive: SendRefreshPassive => {


    }*/



  def  sendRandomRefreshPassive() {

    val neighbor : String = Random.shuffle(activeView).head;

    //TODO not sure verify if nodes are up, ( TIMER to Send)
    val remoteProcess = context.actorSelection(neighbor.concat(ACTOR_NAME))

    val list : List[String] =
      Random.shuffle(passiveView.filter(node => !node.equals(neighbor) && !node.equals(ownAddress)).take(3))

    list.foreach(node => {
      passiveView.filter(!_.equals(node))
    })

    remoteProcess ! ReceiveRefreshSendPassive(ownAddress, list)

}

  def receiveToRefreshSend(senderAddress: String, nodesToRefresh: List[String])  ={

    val remoteProcess = context.actorSelection(senderAddress.concat(ACTOR_NAME))

    val listToSend : List[String] =
      Random.shuffle(passiveView.filter(node => !node.equals(senderAddress) && !node.equals(ownAddress)).take(3))
      listToSend.foreach(node => {
        passiveView.filter(!_.equals(node))
        })

    nodesToRefresh.foreach(newNode =>{
      passiveView = passiveView :+ newNode;
    })

    remoteProcess ! ReceiveRefreshPassive(ownAddress, listToSend)


  }


  def receiveToRefreshPassive(senderAddress: String, nodesToRefresh: List[String]) ={
    nodesToRefresh.foreach(newNode =>{
      passiveView = passiveView :+ newNode;
    })
  }


  def addNodeActiveView(node: String) = {
    if (!activeView.contains(node) && !node.equals(ownAddress)) {
      if(activeView.size == activeViewThreshold){
        dropRandomNodeActiveView();
      }
      activeView = activeView :+ node
    }
    addAlive(node)
    println("active View : ")
    activeView.foreach(aView => println("\t" + aView.toString))

  }




  def dropRandomNodeActiveView() = {
    val remoteProcessAdress : String = Random.shuffle(activeView).head //gives node@ip:port
    val remoteActor = context.actorSelection(remoteProcessAdress.concat(ACTOR_NAME))


    remoteActor ! Disconnect(ownAddress)

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

  def dropRandomNodePassiveView() ={

    val remoteProcessAddress : String = Random.shuffle(passiveView).head;
    passiveView = passiveView.filter(!_.equals(remoteProcessAddress));

  }


  def askPassiveToPromote(disconnectedNode: String) ={

    val nodePromote = Random.shuffle(passiveView.filter(node => !node.equals(disconnectedNode)
      || !node.equals(ownAddress))).head

    if (nodePromote != null){
      val process = context.actorSelection(s"${nodePromote}/user/PartialView")
      

      if (activeView.length == 0) {
        process ! askToPromote("High")
      } else {

        process ! askToPromote("Low")
      }
    }
  }


  def promoteProcessToActiveView(newNode: String) = {
    addNodeActiveView(newNode)
    val process = context.actorSelection(s"${newNode}/user/PartialView")
    if (!activeView.contains(newNode) || !((newNode).equals(ownAddress)))
      process ! AddNew()


  }

  def searchFailedProcesses() = {

    for ((n, t) <- processesAlive) {

      // 5 seconds heartbeat
      if ((System.currentTimeMillis() - t) >= 5000) {
        println("Enter permanent Failure process " + n)
        rUAlive(n)
      }
    }

    for ((n, t) <- uAlive) {
      // more than 10 seconds

      if ((System.currentTimeMillis() - t) >= 7000 && !n.equals(ownAddress)) {
        println("Enter permanent Failure process " + n)
        permanentFailure(n)


      }
    }
  }

  def permanentFailure(nodeAddress: String) = {


    activeView = activeView.filter(!_.equals(nodeAddress))
    passiveView = passiveView.filter(!_.equals(nodeAddress))


    println("node : " + nodeAddress)


    println("new active View : ")
    activeView.foreach(aView => println("\t" + aView.toString))


    val process = context.actorSelection(s"${nodeAddress}/user/Plummtree")
    process ! NeighborDown(nodeAddress)



  }



  def initHeartbeat() = {
    for (h <- activeView) {
      var process = context.actorSelection(s"${h}/user/PartialView")
      process ! Heartbeat()
    }
  }

  def rUAlive(n : String) ={

    //processesAlive -= n
    val timer: Double = System.currentTimeMillis()

    for(n <- activeView){
      var process = context.actorSelection(s"${n}/user/PartialView")
      process ! UThere(n)
    }
  }

  def addAlive(node: String) = {

    val timer: Double = System.currentTimeMillis()
    processesAlive += (node -> timer)
  }




}

object PartialView{
  val props = Props[PartialView]

  case class ReceiveRefreshPassive(senderAddress : String, nodesToRefresh: List[String])

  case class ReceiveRefreshSendPassive(senderAddress : String, nodesToRefresh: List[String])

  case class NodeFailure(nodeAddress: String);

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

  case class askToPromote(priority : String)
}

