package net.uweeisele.kafka.proxy.supplement

import com.sun.net.httpserver.{HttpExchange, HttpHandler}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.net.{HttpURLConnection, URLDecoder}
import java.nio.charset.Charset
import java.util
import java.util.zip.GZIPOutputStream
import scala.jdk.CollectionConverters.CollectionHasAsScala

private class LocalByteArray extends ThreadLocal[ByteArrayOutputStream] {
  override protected def initialValue = new ByteArrayOutputStream(1 << 20)
}

class PrometheusMetricsHttpHandler(registry: CollectorRegistry) extends HttpHandler{

  private final val HEALTHY_RESPONSE = "Exporter is Healthy."
  private val response = new LocalByteArray()

  override def handle(t: HttpExchange): Unit = {
    val query = t.getRequestURI.getRawQuery
    val contextPath = t.getHttpContext.getPath
    val response = this.response.get
    response.reset()
    val osw = new OutputStreamWriter(response, Charset.forName("UTF-8"))
    if ("/-/healthy" == contextPath) osw.write(HEALTHY_RESPONSE)
    else {
      val contentType = TextFormat.chooseContentType(t.getRequestHeaders.getFirst("Accept"))
      t.getResponseHeaders.set("Content-Type", contentType)
      TextFormat.writeFormat(contentType, osw, registry.filteredMetricFamilySamples(parseQuery(query)))
    }
    osw.close()
    if (shouldUseCompression(t)) {
      t.getResponseHeaders.set("Content-Encoding", "gzip")
      t.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0)
      val os = new GZIPOutputStream(t.getResponseBody)
      try response.writeTo(os)
      finally os.close()
    }
    else {
      t.getResponseHeaders.set("Content-Length", String.valueOf(response.size))
      t.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.size)
      response.writeTo(t.getResponseBody)
    }
    t.close()
  }

  protected def shouldUseCompression(exchange: HttpExchange): Boolean = {
    val encodingHeaders = exchange.getRequestHeaders.get("Accept-Encoding")
    if (encodingHeaders == null) return false
    for (encodingHeader <- encodingHeaders.asScala) {
      val encodings = encodingHeader.split(",")
      for (encoding <- encodings) {
        if (encoding.trim.equalsIgnoreCase("gzip")) return true
      }
    }
    false
  }

  protected def parseQuery(query: String): util.Set[String] = {
    val names = new util.HashSet[String]
    if (query != null) {
      val pairs = query.split("&")
      for (pair <- pairs) {
        val idx = pair.indexOf("=")
        if (idx != -1 && URLDecoder.decode(pair.substring(0, idx), "UTF-8") == "name[]") names.add(URLDecoder.decode(pair.substring(idx + 1), "UTF-8"))
      }
    }
    names
  }
}
