package app

import akka.actor.{ActorSystem, DeadLetter, Props}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import layers.EpidemicBroadcastTree.MainPlummtree
import layers.EpidemicBroadcastTree.MainPlummtree.Broadcast
import layers.MembershipLayer.PartialView
import layers.MyDeadLetterListener

object Process extends App {

    val SYSTEM_NAME = "node"

    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0);
    val config = ConfigFactory.parseString(
      s"""
        akka.remote.netty.tcp.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [node]"))
      .withFallback(ConfigFactory.load());

    //Creates an actor system -- this is actually a process
    val process = ActorSystem(SYSTEM_NAME, config);

    val listener = process.actorOf(Props[MyDeadLetterListener])
    process.eventStream.subscribe(listener, classOf[DeadLetter])

    //Create new actor as child of this context
    val ownAddress = getOwnAddress(port.toInt);
    val partialView = process.actorOf(Props[PartialView], "PartialView");
    val plummtree = process.actorOf(Props[MainPlummtree], "Plummtree");
    //e suposto saber ja o contact node assim ??
    //Como receber o nome do contact node??  actorSystemName@10.0.0.1:2552/user/actorName -> Node@127.0.0.1:9999/user/partialview
    var contactNode = args(1);
    partialView ! PartialView.Init(ownAddress, contactNode);
    plummtree ! MainPlummtree.Init();


  def getOwnAddress(port: Int) = {
      val address = config.getAnyRef("akka.remote.netty.tcp.hostname")
      val port = config.getAnyRef("akka.remote.netty.tcp.port")

      s"akka.tcp://${process.name}@${address}:${port}"
    }

}
