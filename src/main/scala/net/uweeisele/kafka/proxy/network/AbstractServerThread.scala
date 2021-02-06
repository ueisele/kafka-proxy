package net.uweeisele.kafka.proxy.network

import com.typesafe.scalalogging.LazyLogging

import java.nio.channels.SocketChannel
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

abstract class AbstractServerThread extends Runnable with LazyLogging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop
   */
  def initiateShutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
  }

  /**
   * Wait for the thread to completely shutdown
   */
  def awaitShutdown(): Unit = shutdownLatch.await

  /**
   * Returns true if the thread is completely started
   */
  def isStarted(): Boolean = startupLatch.getCount == 0

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel`.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      logger.debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress}")
      try {channel.socket().close()} catch { case e: Throwable => logger.error(e.getMessage, e) }
      try {channel.close()} catch { case e: Throwable => logger.error(e.getMessage, e) }
    }
  }
}