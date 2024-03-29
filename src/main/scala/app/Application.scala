package app

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import layers.EpidemicBroadcastTree.MainPlummtree.Stats


object Application extends App {

  val config = ConfigFactory.load.getConfig("ApplicationConfig")
  val system = ActorSystem("akkaSystem", config)
  val app = system.actorOf(Props[app], "app")

  while (true) {

    val line = scala.io.StdIn.readLine()
    var cmd: Array[String] = line.split("\\s")
    cmd(0) match {
      case "ms" if (cmd.length == 2) => Stats(cmd(1))
      case _ => println("Wrong command")
    }

  }


  def stats(process: String) = {
    app ! Stats(process)
  }


  class app extends Actor {

    override def receive = {
      case Stats(p) => {
        val process = system.actorSelection(s"${p}/user/MainPlummtree")
        process ! Stats

      }

      case stats: ShowStats => {
        println("Messages Statistics from: " + sender.path.address.toString)
        println ()

        println ("\t - Messages Sent: " + stats.totalMessagesSent)
        println ("\t - Messages Received: " + stats.totalMessagesReceived)


      }


    }




  }
  case class ShowStats(totalMessagesSent: Int, totalMessagesReceived: Int)

}