package layers.MembershipLayer

import java.util.concurrent.{TimeUnit, TimeoutException}

import akka.pattern.ask
import akka.actor.{Actor, Props, Timers}
import akka.util.Timeout
import layers.EpidemicBroadcastTree.MainPlummtree
import layers.EpidemicBroadcastTree.MainPlummtree.{NeighborDown, NeighborUp}
import layers.MembershipLayer.PartialView._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global


class PartialView extends Actor with Timers
{

  val SYSTEM_NAME :String = "node"
  val ACTOR_NAME :String = "/user/PartialView"
  var ownAddress : String = "" //actor ref
  var activeView: List[String] = List.empty //list of node@host:port
  var passiveView: List[String] = List.empty
  val activeViewThreshold : Int = 4
  val passiveViewThreshold : Int = 35
  val ARWL : Int = 3; //Active Random Walk Length
  val PRWL : Int  = 2; //Passive Random Walk Length
  var processesAlive : Map[String, Double] = Map[String, Double]()
  var uAlive : Map[String, Double] = Map[String, Double]()

  //TODO: So adicionar quando se recebe msg positiva do outro node

  override def receive: PartialFunction[Any, Unit] = {
    case message: PartialView.Init =>
      ownAddress = message.ownAddress
      var done : Boolean = false
      var attempt : Int = 0

      if (!message.contactNode.equals("")) {
          do{
            try {
              attempt += 1
              val conctactActor = context.actorSelection(message.contactNode.concat(ACTOR_NAME))
              addNodeActiveView(message.contactNode)
              addAlive(message.contactNode)
              conctactActor ! Join(message.ownAddress)
              done = true
            }catch {
              case _ : TimeoutException => printf("Node did not reponse... try " + attempt + " out of 3")
            }

          }while(!done && attempt <= 3 )
          if(!done) {printf("Vou-me matar mas não sei como lelel \n") }
        }


      val plumm = context.actorSelection("/user/MainPlummtree")
      plumm ! MainPlummtree.Init(ownAddress)

      context.system.scheduler.schedule(60 seconds, 5 seconds)(heartbeatProcedure())
      //context.system.scheduler.schedule(60 seconds, 40 seconds)(passiveViewShufflrProcedure())

    case join: Join =>
      printf("Recebi Join de: " + join.newNodeAddress +"\n")
      addNodeActiveView(join.newNodeAddress)
      addAlive(join.newNodeAddress)
      val plummTreeActor = context.actorSelection("/user/MainPlummtree")
      plummTreeActor ! NeighborUp(join.newNodeAddress)

      activeView.filter(node => !node.equals(join.newNodeAddress)).foreach(node => {
        val remoteProcess = context.actorSelection(node.concat(ACTOR_NAME))
        remoteProcess ! ForwardJoin(join.newNodeAddress, ARWL, ownAddress)
      })

    case forwardJoin: ForwardJoin =>
      if (forwardJoin.arwl == 0 || activeView.size == 1) {

        val process = context.actorSelection(s"${forwardJoin.newNode}/user/PartialView")
        process ! NeighborRequest(1, ownAddress)
        if(addNodeActiveView(forwardJoin.newNode)) {
          addAlive(forwardJoin.newNode)
          val plummTreeActor = context.actorSelection("/user/MainPlummtree")
          plummTreeActor ! NeighborUp(forwardJoin.newNode)
        }
      }else{
        if(forwardJoin.arwl == PRWL){
          printf("Vou adicionar: " + forwardJoin.newNode + " a passiva\n")
          addNodePassiveView(forwardJoin.newNode)
        }
        val neighborAddress: String = Random.shuffle(activeView.filter(n => !n.equals(forwardJoin.newNode))).head
        val neighborMembershipActor = context.actorSelection(neighborAddress.concat(ACTOR_NAME))
        neighborMembershipActor ! ForwardJoin(forwardJoin.newNode, forwardJoin.arwl - 1, ownAddress)
      }


    case disconnect: Disconnect =>
      if(activeView.contains(disconnect.disconnectNode)) {
        activeView = activeView.filter(!_.equals(disconnect.disconnectNode))
        addNodePassiveView(disconnect.disconnectNode)
        processesAlive -= disconnect.disconnectNode

        val plummtree = context.actorSelection("/user/MainPlummtree")
        plummtree ! NeighborDown(disconnect.disconnectNode)
      }

    case getPeers: getPeers =>
      val split_Value : Int = math.min(getPeers.fanout, activeView.size)
      val peers : List[String] = activeView.splitAt(split_Value)._1
      sender ! peers

    /* ----------------------------------------------------------------------------------------*/

    case _: HeartbeatProcedure =>
      //printf("Vou Inspecionar\n")
      for ((n, t) <- processesAlive) {
        // 5 seconds heartbeat
        if ((System.currentTimeMillis() - t) >= 5000) {
        //  println("Vou ver se este gajo morreu: " + n)
          rUAlive(n)
        }
      }

      for ((n, t) <- uAlive) {
        // more than 10 seconds
        if ((System.currentTimeMillis() - t) >= 7000 && !n.equals(ownAddress)) {
          //println("Enter permanent Failure process " + n)
          permanentFailure(n)
        }
      }

    case _: UThere =>
      sender ! ImHere(ownAddress)

    case imHere: ImHere =>
      //printf("Ta vivo: " +sender.path.address.toString+ "\n" )
      if(activeView.contains(imHere.nodeAddress)) {
        uAlive -= sender.path.address.toString
        val timer: Double = System.currentTimeMillis()
        processesAlive += (sender.path.address.toString -> timer)

      }else{
        sender ! Disconnect(ownAddress)
      }

    case neighborRequest: NeighborRequest =>
      if(neighborRequest.priority == 1 || activeView.size < activeViewThreshold)  {
       if(addNodeActiveView(neighborRequest.nodeAddress)) {
         addAlive(neighborRequest.nodeAddress)
       }
      }

    /*--------------------------------------------------------------------------------------------------*/

    case _: PassiveViewProcedure =>

      if(passiveView.nonEmpty) {
        val neighbor: String = Random.shuffle(activeView).head
        val remoteProcess = context.actorSelection(neighbor.concat(ACTOR_NAME))
        val toSend: List[String] = Random.shuffle(passiveView.filter(node => !node.equals(neighbor)).take(3))
        passiveView = passiveView.diff(toSend)
        remoteProcess ! RefreshPassiveView(ownAddress, toSend)
      }

    case receiveRefreshSendPassive: RefreshPassiveView =>
      val toSend : List[String] = Random.shuffle(passiveView.filter(node => !node.equals(receiveRefreshSendPassive.senderAddress)).take(3))
      passiveView = passiveView.diff(toSend)
      passiveView ++= receiveRefreshSendPassive.nodesToRefresh
      sender ! RefreshPassiveViewReply(toSend)

    case reply: RefreshPassiveViewReply =>
      passiveView ++ reply.toSend

  }


  /** Support Methods  **/
  def addNodeActiveView(node: String): Boolean = {
    if (!activeView.contains(node) && !node.equals(ownAddress)) {
      if(activeView.size == activeViewThreshold){
        dropRandomNodeActiveView()
      }
      activeView = activeView :+ node
      return true
    }

    println("active View : ")
    activeView.foreach(aView => println("\t" + aView.toString))
    false
  }


  def dropRandomNodeActiveView(): Unit = {
    val remoteProcessAdress : String = Random.shuffle(activeView).head
    //println("vou remover da active: " + remoteProcessAdress)
    val remoteActor = context.actorSelection(remoteProcessAdress.concat(ACTOR_NAME))
    remoteActor ! Disconnect(ownAddress)
    activeView = activeView.filter(!_.equals(remoteProcessAdress))
    addNodePassiveView(remoteProcessAdress)
  }

  def addNodePassiveView(nodeAddress: String): Unit = {
    if (!passiveView.contains(nodeAddress) && !activeView.contains(nodeAddress) && !nodeAddress.equals(ownAddress)) {
      if(passiveView.size == passiveViewThreshold) {
        dropRandomNodePassiveView()
      }
      passiveView = passiveView :+ nodeAddress
      //println("node added to passiveView : " + nodeAddress)
    }
  }

  def dropRandomNodePassiveView(): Unit ={

    val remoteProcessAddress : String = Random.shuffle(passiveView).head
    passiveView = passiveView.filter(!_.equals(remoteProcessAddress))
   // printf(remoteProcessAddress + " removido da passive view")
  }

  def promoteProcessToActiveView(newNode: String): Boolean = {
    val process = context.actorSelection(s"$newNode/user/PartialView")
    val priority = if(activeView.isEmpty) 1 else 0
    process ! NeighborRequest(priority, ownAddress)
    addNodeActiveView(newNode)
    addAlive(newNode)
    true
  }


  def promoteRandomProcessToActiveView(): Unit = {

    if(passiveView.nonEmpty) {
      var toPromote: String = ""
      do {
        toPromote = Random.shuffle(passiveView).head
        passiveView = passiveView.filter(!_.equals(toPromote))
      } while (!promoteProcessToActiveView(toPromote))
    }
  }

  def permanentFailure(nodeAddress: String): Unit = {
    printf("O Node: " +nodeAddress +" morreu! \n")
    activeView = activeView.filter(!_.equals(nodeAddress))
    passiveView = passiveView.filter(!_.equals(nodeAddress))
    uAlive -= nodeAddress
    //println("node : " + nodeAddress)
    //println("new active View : ")
    activeView.foreach(aView => println("\t" + aView.toString))
    val plummTree = context.actorSelection("/user/MainPlummtree")
    plummTree ! NeighborDown(nodeAddress)
    promoteRandomProcessToActiveView()
  }


  def rUAlive(nodeAddr : String): Unit = {
    try {

      val currentTime = System.currentTimeMillis()
      processesAlive -= nodeAddr
      uAlive += nodeAddr -> currentTime
      val process = context.actorSelection(s"$nodeAddr/user/PartialView")
      process ! UThere

    }catch{
      case timeoutEx : TimeoutException => printf("Nao respondeu!")
    }
  }

  def addAlive(node: String): Unit = {
    val timer: Double = System.currentTimeMillis()
    processesAlive += (node -> timer)
  }

  def heartbeatProcedure(): Unit = {
    //printf("Vou Inspecionar\n")
    for ((n, t) <- processesAlive) {
      // 5 seconds heartbeat
      if ((System.currentTimeMillis() - t) >= 5000) {
      //  println("Vou ver se este gajo morreu: " + n)
        rUAlive(n)
      }
    }

    for ((n, t) <- uAlive) {
      // more than 10 seconds
      if ((System.currentTimeMillis() - t) >= 15000 && !n.equals(ownAddress)) {
       // println("Enter permanent Failure process " + n)
        permanentFailure(n)
      }
    }
  }


  def passiveViewShufflrProcedure(): Unit = {
    implicit val timeout: Timeout = Timeout(FiniteDuration(2, TimeUnit.SECONDS))
    val neighbor: String = Random.shuffle(activeView).head
    val remoteProcess = context.actorSelection(neighbor.concat(ACTOR_NAME))
    val toSend: List[String] = Random.shuffle(passiveView.filter(node => !node.equals(neighbor)).take(3))
    passiveView = passiveView.diff(toSend)
    val future = remoteProcess ? RefreshPassiveView(ownAddress, toSend)
    val newPassiveNodes = Await.result(future, timeout.duration).asInstanceOf[List[String]]
    passiveView ++= newPassiveNodes

  }



}

object PartialView{
  val props: Props = Props[PartialView]

  case class PassiveViewProcedure()

  case class RefreshPassiveView(senderAddress : String, nodesToRefresh: List[String])

  case class RefreshPassiveViewReply(toSend: List[String])

  case class ImHere(nodeAddress: String)

  case class UThere()

  case class NeighborRequest(priority: Int, nodeAddress : String)

  case class HeartbeatProcedure()

  case class Init (ownAddress : String, contactNode : String)

  case class Join (newNodeAddress: String)

  case class ForwardJoin(newNode: String, arwl: Int, senderAddress: String)

  case class Disconnect (disconnectNode: String)

  case class getPeers(fanout: Integer)

}

