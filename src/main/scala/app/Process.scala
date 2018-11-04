package app

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import layers.{PartialView}

object Process extends App {


    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(
      s"""
        akka.remote.netty.tcp.port=$port
        """)
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [node]"))
      .withFallback(ConfigFactory.load())

    //Creates an actor system
    val system = ActorSystem("ClusterSystem", config)

    //Create new actor as child of this context
    val ownAddress = getOwnAddress(port.toInt)
    val partialView = system.actorOf(Props[PartialView], "PartialView")
    //e suposto saber ja o contact node assim ??
    var contactNode = args(1)
    println(ownAddress)
    partialView ! Init(ownAddress, contactNode)

  def getOwnAddress(port: Int) = {
      val address = config.getAnyRef("akka.remote.netty.tcp.hostname")
      val port = config.getAnyRef("akka.remote.netty.tcp.port")

      s"akka.tcp://${system.name}@${address}:${port}"
    }


}
