package net.uweeisele.kafka.proxy

import com.typesafe.scalalogging.LazyLogging
import joptsimple.OptionParser
import org.apache.kafka.common.utils._

import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaProxyStartable extends LazyLogging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    if (args.length == 0 || args.contains("--help")) {
      System.err.println("USAGE: java [options] %s proxy.properties [--override property=value]*".format(classOf[KafkaProxy].getSimpleName()))
      optionParser.printHelpOn(System.err)
      System.exit(1)
    }

    if (args.contains("--version")) {
      System.exit(0)
    }

    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        System.err.println("Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
        optionParser.printHelpOn(System.err)
        System.exit(1)
      }

      props.putAll(parseKeyValueArgs(options.valuesOf(overrideOpt).asScala))
    }
    props
  }

  /**
   * Parse key-value pairs in the form key=value
   * value may contain equals sign
   */
  def parseKeyValueArgs(args: Iterable[String], acceptMissingValue: Boolean = true): Properties = {
    val splits = args.map(_.split("=", 2)).filterNot(_.length == 0)

    val props = new Properties
    for (a <- splits) {
      if (a.length == 1 || (a.length == 2 && a(1).isEmpty())) {
        if (acceptMissingValue) props.put(a(0), "")
        else throw new IllegalArgumentException(s"Missing value for key ${a(0)}")
      }
      else props.put(a(0), a(1))
    }
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      val serverProps = getPropsFromArgs(args)
      val kafkaProxyStartable = KafkaProxy.fromProps(serverProps)

      try {
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          logger.warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      // attach shutdown handler to catch terminating signals as well as normal termination
      Runtime.getRuntime.addShutdownHook(KafkaThread.nonDaemon("kafka-shutdown-hook", () => {
        try kafkaProxyStartable.shutdown()
        catch {
          case _: Throwable =>
            logger.error("Halting Kafka.")
            // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
            Runtime.getRuntime.halt(1)
        }
      }))

      kafkaProxyStartable.startup()
      kafkaProxyStartable.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        logger.error("Exiting Kafka due to fatal exception", e)
        System.exit(1)
    }
    System.exit(0)
  }
}