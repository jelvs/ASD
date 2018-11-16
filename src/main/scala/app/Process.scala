package app

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import layers.EpidemicBroadcastTree.MainPlummtree
import layers.MembershipLayer.PartialView


object Process extends App {

    var port = 2552
    if (args.length != 0) {
        port = args(0).toInt
    }

    val config = configure()
    val system = ActorSystem("SystemName", config)
    val ownAddress = getOwnAddress(port)
    val partialView = system.actorOf(Props[PartialView], "PartialView")
    val plummtree = system.actorOf(Props[MainPlummtree], "Plummtree")

    var contactNode = ""
    if (args.length > 1) {
        contactNode = s"akka.tcp://${system.name}@" + args(1)
    }

    println(ownAddress)


    partialView ! PartialView.Init(ownAddress, contactNode)




    //    println("Contact Node: " + contactNode)


    def configure(): Config = {

        ConfigFactory.load.getConfig("Process").withValue("akka.remote.netty.tcp.port",
            ConfigValueFactory.fromAnyRef(port))

    }

    def getOwnAddress(port: Int) = {
        val address = config.getAnyRef("akka.remote.netty.tcp.hostname")
        val port = config.getAnyRef("akka.remote.netty.tcp.port")

        s"akka.tcp://${system.name}@${address}:${port}"
    }




    /*val SYSTEM_NAME = "node"

    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "2552" else args(0)
    val config = configure()

    //Creates an actor system -- this is actually a process
    val process = ActorSystem(SYSTEM_NAME, config)

    //val listener = process.actorOf(Props[MyDeadLetterListener])
    //process.eventStream.subscribe(listener, classOf[DeadLetter])

    //Create new actor as child of this context
    val ownAddress = getOwnAddress(port.toInt);
    val partialView = process.actorOf(Props[PartialView], "PartialView")
    val plummtree = process.actorOf(Props[MainPlummtree], "Plummtree")
    //e suposto saber ja o contact node assim ??
    //Como receber o nome do contact node??  actorSystemName@10.0.0.1:2552/user/actorName -> Node@127.0.0.1:9999/user/partialview
    val contactNode = args(1)
    partialView ! PartialView.Init(ownAddress, contactNode)


    def configure(): Config = {
        ConfigFactory.load.getConfig("Process").withValue("akka.remote.netty.tcp.port",
            ConfigValueFactory.fromAnyRef(port))
    }


    def getOwnAddress(port: Int) = {
      val address = config.getAnyRef("akka.remote.netty.tcp.hostname")
      val port = config.getAnyRef("akka.remote.netty.tcp.port")

      s"akka.tcp://${process.name}@${address}:${port}"
    }
*/
}

