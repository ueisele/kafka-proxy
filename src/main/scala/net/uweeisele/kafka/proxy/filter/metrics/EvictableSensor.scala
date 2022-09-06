package net.uweeisele.kafka.proxy.filter.metrics

import io.micrometer.core.instrument.{Meter, MeterRegistry, Tags}

import java.io.Closeable
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES}

object EvictableSensor {
  def apply[M <: Meter](meterBuilder:(Tags, String) => (MeterRegistry) => M, ttl: FiniteDuration = Duration(5, MINUTES))(implicit meterRegistry: MeterRegistry): EvictableSensor[M] =
    new EvictableSensor[M](meterBuilder, ttl, meterRegistry)
}

class EvictableSensor[M <: Meter](meterBuilder: (Tags, String) => (MeterRegistry) => M, ttl: FiniteDuration, meterRegistry: MeterRegistry) extends ((Tags, String) => M) with Evictable with Closeable {

  private val meterLastAccess: concurrent.Map[Meter.Id, AtomicReference[Instant]] = concurrent.TrieMap()

  override def apply(tags: Tags, name: String = ""): M = {
    val meter = meterBuilder(tags, name)(meterRegistry)
    meterLastAccess.getOrElseUpdate(meter.getId, new AtomicReference()).set(Instant.now)
    meter
  }

  override def evict(): Unit = {
    val evictBefore = Instant.now.minusMillis(ttl.toMillis)
    meterLastAccess.filterInPlace { (meterId, lastAccess) =>
      if (lastAccess.get().isBefore(evictBefore)) {
        meterRegistry.remove(meterId)
        false
      } else {
        true
      }
    }
  }

  override def close(): Unit = {
    meterLastAccess.filterInPlace { (meterId, _) =>
      meterRegistry.remove(meterId)
      false
    }
  }

}
