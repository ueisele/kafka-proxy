package net.uweeisele.kafka.proxy.supplement

import com.sun.net.httpserver.HttpHandler
import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.util.NamedThreadFactory

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, FiniteDuration, NANOSECONDS}

class HttpServer(bindAddress: InetSocketAddress,
                 backlog: Int = 0,
                 threads: Int = 1,
                 threadNamePrefix: String = "http-server") extends LazyLogging {

  private val executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory(threadNamePrefix))
  private val httpServer = com.sun.net.httpserver.HttpServer.create(bindAddress, backlog)
  httpServer.setExecutor(executor)

  def start(): HttpServer = {
    ensureNotStopped()
    httpServer.start()
    logger.info(s"Started HttpServer $threadNamePrefix on bind address $bindAddress with $threads threads.")
    this
  }

  def shutdown(): Unit = {
    shutdown(Duration(Long.MaxValue, NANOSECONDS))
  }

  def shutdown(waitTime: FiniteDuration): Unit = {
    ensureNotStopped()
    val startNs = System.nanoTime()
    httpServer.stop(waitTime.toSeconds.toInt)
    executor.shutdown()
    executor.awaitTermination(waitTime.toNanos - (System.nanoTime() - startNs), NANOSECONDS)
    logger.info(s"Stopped HttpServer $threadNamePrefix on bind address $bindAddress.")
  }

  def addHandler(path: String, handler: HttpHandler): HttpServer = {
    httpServer.createContext(path, handler)
    this
  }

  private def ensureNotStopped(): Unit =
    if (executor.isShutdown) {
      throw new IllegalStateException("Http server has been already stopped!")
    }
}
