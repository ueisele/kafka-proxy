package net.uweeisele.kafka.proxy.filter.metrics

import java.io.Closeable
import scala.collection.mutable.ListBuffer

class EvictableContainer extends Evictable with Closeable {

  private val listBuffer = ListBuffer[Evictable with Closeable]()

  def add[I <: Evictable with Closeable](item: I): I = {
    listBuffer.addOne(item)
    item
  }

  override def evict(): Unit = listBuffer.foreach(e => e.evict())

  override def close(): Unit = listBuffer.foreach(e => e.close())
}
