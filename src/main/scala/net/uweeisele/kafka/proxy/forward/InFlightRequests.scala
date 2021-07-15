/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *//*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.uweeisele.kafka.proxy.forward

import net.uweeisele.kafka.proxy.network.RequestChannel
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.requests.RequestHeader

import java.util
import java.util._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

class InFlightRequest(val request: RequestChannel.Request,
                      val connectionId: String,
                      val onCompleteCallback: Option[Send => Unit],
                      val sendTimeNanos: Long,
                      val requestTimeoutMs: Long) {

  private var sent = false

  val header: RequestHeader = request.header
  val createdTimeNanos: Long = request.startTimeNanos

  def isSent: Boolean = sent

  def setSent(): Unit = sent = true

  override def toString: String = new StringJoiner(", ", classOf[InFlightRequest].getSimpleName + "[", "]")
    .add("request=" + request)
    .add("header=" + header)
    .add("connectionId='" + connectionId + "'")
    .add("sent='" + sent + "'")
    .add("createdTimeNanos=" + createdTimeNanos)
    .add("sendTimeNanos=" + sendTimeNanos)
    .add("requestTimeoutMs=" + requestTimeoutMs)
    .toString
}

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
class InFlightRequests {
  private val requests = new util.HashMap[String, util.Deque[InFlightRequest]]
  /** Thread safe total number of in flight requests. */
  private val inFlightRequestCount = new AtomicInteger(0)

  /**
   * Add the given request to the queue for the connection it was directed to
   */
  def add(request: InFlightRequest): Int = {
    val connectionId = request.connectionId
    var reqs = this.requests.get(connectionId)
    if (reqs == null) {
      reqs = new util.ArrayDeque[InFlightRequest]
      this.requests.put(connectionId, reqs)
    }
    reqs.addFirst(request)
    inFlightRequestCount.incrementAndGet
  }

  /**
   * Get the request queue for the given connection
   */
  private def requestQueue(connectionId: String) = {
    val reqs = requests.get(connectionId)
    if (reqs == null || reqs.isEmpty) throw new IllegalStateException("There are no in-flight requests for connection " + connectionId)
    reqs
  }

  def connections: util.Set[String] = requests.keySet()

  def unset(connectionId: String): util.stream.Stream[InFlightRequest] = {
    requests.get(connectionId).stream().filter( request => !request.isSent)
  }

  /**
   * Get the oldest request (the one that will be completed next) for the given connection
   */
  def completeNext(connectionId: String): InFlightRequest = {
    val inFlightRequest = requestQueue(connectionId).pollLast
    inFlightRequestCount.decrementAndGet
    inFlightRequest
  }

  /**
   * Get the last request we sent to the given connection (but don't remove it from the queue)
   *
   * @param connectionId The connectionId
   */
  def lastSent(connectionId: String): InFlightRequest = requestQueue(connectionId).peekFirst

  /**
   * Complete the last request that was sent to a particular connection.
   *
   * @param connectionId The connection the request was sent to
   * @return The request
   */
  def completeLastSent(connectionId: String): InFlightRequest = {
    val inFlightRequest = requestQueue(connectionId).pollFirst
    inFlightRequestCount.decrementAndGet
    inFlightRequest
  }

  /**
   * Return the number of in-flight requests directed at the given connection
   *
   * @param connectionId The connectionId
   * @return The request count.
   */
  def count(connectionId: String): Int = {
    val queue = requests.get(connectionId)
    if (queue == null) 0
    else queue.size
  }

  /**
   * Return true if there is no in-flight request directed at the given node and false otherwise
   */
  def isEmpty(connectionId: String): Boolean = {
    val queue = requests.get(connectionId)
    queue == null || queue.isEmpty
  }

  /**
   * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
   */
  def count: Int = inFlightRequestCount.get

  /**
   * Return true if there is no in-flight request and false otherwise
   */
  def isEmpty: Boolean = {
    import scala.jdk.CollectionConverters._
    for (deque <- this.requests.values.asScala) {
      if (!deque.isEmpty) return false
    }
    true
  }

  /**
   * Clear out all the in-flight requests for the given connection and return them
   *
   * @param connectionId The connectionId
   * @return All the in-flight requests for that connection that have been removed
   */
  def clearAll(connectionId: String): Seq[InFlightRequest] = {
    val reqs = requests.get(connectionId)
    if (reqs == null) Seq()
    else {
      val clearedRequests = requests.remove(connectionId)
      inFlightRequestCount.getAndAdd(-clearedRequests.size)
      import scala.jdk.CollectionConverters._
      clearedRequests.asScala.toSeq
    }
  }

  private def hasExpiredRequest(nowNanos: Long, deque: util.Deque[InFlightRequest]): Boolean = {
    import scala.jdk.CollectionConverters._
    for (request <- deque.asScala) {
      val timeSinceSend = Math.max(0, nowNanos - request.createdTimeNanos) / 1000000
      if (timeSinceSend > request.requestTimeoutMs) return true
    }
    false
  }

  /**
   * Returns a list of nodes with pending in-flight request, that need to be timed out
   *
   * @param nowNanos current time in nanoseconds
   * @return list of connections
   */
  def connectionsWithTimedOutRequests(nowNanos: Long): Seq[String] = {
    val nodeIds = ListBuffer[String]()
    import scala.jdk.CollectionConverters._
    for (requestEntry <- requests.entrySet.asScala) {
      val nodeId = requestEntry.getKey
      val deque = requestEntry.getValue
      if (hasExpiredRequest(nowNanos, deque)) nodeIds += nodeId
    }
    nodeIds.toSeq
  }
}