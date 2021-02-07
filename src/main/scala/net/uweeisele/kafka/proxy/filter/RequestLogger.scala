package net.uweeisele.kafka.proxy.filter

import net.uweeisele.kafka.proxy.network.{AbstractServerThread, RequestChannel}
import org.apache.kafka.common.utils.KafkaThread

class RequestLogger(requestChannel: RequestChannel) extends AbstractServerThread {

  override def wakeup(): Unit = ???

  override def run(): Unit = {
    startupComplete()
    try {
      while (isRunning) {
        requestChannel.receiveRequest(500) match {
          case request: RequestChannel.Request => println(request)
          case _ => //continue
        }
      }
    } catch {
      case _: Throwable => shutdownComplete()
    }
  }

  def start(): Unit = {
    if (!isStarted()) {
      KafkaThread.nonDaemon(
        s"kafka-logger",
        this
      ).start()
      awaitStartup()
    }
  }
}
