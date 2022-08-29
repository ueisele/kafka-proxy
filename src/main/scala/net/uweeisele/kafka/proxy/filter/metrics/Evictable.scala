package net.uweeisele.kafka.proxy.filter.metrics

trait Evictable {
  def evict(): Unit
}
