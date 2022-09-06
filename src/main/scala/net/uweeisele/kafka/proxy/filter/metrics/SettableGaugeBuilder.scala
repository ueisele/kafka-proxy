package net.uweeisele.kafka.proxy.filter.metrics

import io.micrometer.core.instrument.{Gauge, Meter, MeterRegistry, Tags}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent

object SettableGaugeBuilder {
  def apply(nameBuilder: String => String, meterConfigurer: Gauge.Builder[AtomicReference[Double]] => Gauge.Builder[AtomicReference[Double]] = b => b): SettableGaugeBuilder =
    new SettableGaugeBuilder(nameBuilder, meterConfigurer)
}

class SettableGaugeBuilder(nameBuilder: String => String, meterConfigurer: Gauge.Builder[AtomicReference[Double]] => Gauge.Builder[AtomicReference[Double]] = b => b) extends ((Tags, String) => (MeterRegistry) => SettableGauge) {

  private val valueRefs: concurrent.Map[Tags, AtomicReference[Double]] = concurrent.TrieMap()

  override def apply(tags: Tags, name: String): Function[MeterRegistry, SettableGauge] = (meterRegistry: MeterRegistry) => {
    val valueRef = valueRefs.getOrElseUpdate(tags, new AtomicReference[Double](0.0))
    val meter = meterConfigurer(Gauge.builder(nameBuilder(name), valueRef, (v: AtomicReference[Double]) => v.get()).tags(tags)).register(meterRegistry)
    new SettableGauge(meter, valueRef, tags, valueRefs)
  }
}

class SettableGauge(gauge: Gauge, valueRef: AtomicReference[Double], tags: Tags, valueRefs: concurrent.Map[Tags, AtomicReference[Double]]) extends Gauge {

  override def getId: Meter.Id = gauge.getId

  override def value(): Double = gauge.value

  def set(value: Double): Unit = valueRef.set(value)

  override def close(): Unit = {
    super.close()
    valueRefs.remove(tags)
  }
}