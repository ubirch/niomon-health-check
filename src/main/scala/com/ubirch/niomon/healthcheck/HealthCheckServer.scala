package com.ubirch.niomon.healthcheck

import java.net.{InetSocketAddress, Socket}
import java.util.Collections

import akka.kafka.scaladsl.Consumer.Control
import com.avsystem.commons.rpc.AsRaw
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.StrictLogging
import com.ubirch.niomon.healthcheck.HealthCheckServer._
import io.prometheus.client.CollectorRegistry
import io.udash.rest.openapi._
import io.udash.rest.openapi.adjusters.adjustSchema
import io.udash.rest.raw.{HttpBody, JsonValue, RestResponse}
import io.udash.rest.{DefaultRestApiCompanion, GET, RestDataCompanion, RestDataWrapperCompanion}
import net.logstash.logback.argument.StructuredArguments.v
import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import org.apache.kafka.common.{Metric, MetricName}
import org.json4s.JsonAST._
import org.json4s.JsonDSL
import org.json4s.jackson.JsonMethods

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success}

/** Health check result. */
case class CheckResult(checkName: String, success: Boolean, payload: JValue)

/**
 * Health check server. Create an instance, add some checks, and then start the server using `.run(port)`.
 * Checks are split into liveness and readiness checks. Refer to kubernetes documentation for the definition of the
 * two.
 */
class HealthCheckServer(
  var livenessChecks: Map[String, CheckerFn],
  var readinessChecks: Map[String, CheckerFn]
) extends HealthCheckApi with StrictLogging {
  implicit val ec: ExecutionContext = ExecutionContext.global

  private def doCheck(checks: Map[String, CheckerFn]): Future[(Boolean, JValue)] = {
    Future.sequence(checks.map { case (checkName, checkerFn) =>
      checkerFn(ec).transform {
        case s@Success(_) => s
        case Failure(exception) => Success(CheckResult(
          checkName = checkName,
          success = false,
          payload = JObject(("status", JString(s"exception: ${exception.getMessage}")))
        ))
      }
    })
      .map { checks =>
        checks.foldLeft((true, JObject())) { case ((success, o), check) =>
          (success && check.success, o.merge(JsonDSL.pair2jvalue((check.checkName, check.payload))))
        }
      }
  }

  def setLivenessCheck(name: String)(checkFn: CheckerFn): Unit = {
    livenessChecks += name -> checkFn
  }

  def setLivenessCheck(nameAndCheckFn: (String, CheckerFn)): Unit = {
    livenessChecks += nameAndCheckFn
  }

  def setReadinessCheck(name: String)(checkFn: CheckerFn): Unit = {
    readinessChecks += name -> checkFn
  }

  def setReadinessCheck(nameAndCheckFn: (String, CheckerFn)): Unit = {
    readinessChecks += nameAndCheckFn
  }

  private def endpoint(checks: Map[String, CheckerFn]): Future[HealthCheckResponse] = {
    doCheck(checks).map { case (success, payload) =>
      val serializedPayload = JsonValue(JsonMethods.compact(payload))
      if (success) {
        HealthCheckSuccess(serializedPayload)
      } else {
        logger.warn(s"replying with failed health check: [{}]", v("healthCheckBody", serializedPayload))
        HealthCheckFailure(serializedPayload)
      }
    }
  }

  override def live(): Future[HealthCheckResponse] = endpoint(livenessChecks)

  override def ready(): Future[HealthCheckResponse] = endpoint(readinessChecks)

  private var server: JettyServer = _

  /** Start a Jetty server that serves the health checks on `port`. */
  def run(port: Int): Unit = {
    if (server != null) join()

    server = new JettyServer(this, HealthCheckApi.openapiMetadata.openapi(
      Info("Health Check API", "1.0.0"),
      servers = List(Server(s"http://localhost:$port/health"))
    ), port)

    server.start()
  }

  def join(): Unit = if (server != null) server.join()

  def stop(): Unit = if (server != null) server.stop()

}

object HealthCheckServer {
  type CheckerFn = ExecutionContext => Future[CheckResult]

  implicit def f0ToCheckerFn(a: () => Future[CheckResult]): CheckerFn = _ => a()
}

/** A collection of common checks */
object Checks extends StrictLogging {
  def notInitialized(name: String): (String, CheckerFn) = {
    // I'm pretty sure that "not-initialized" should be counted as a success - it's a normal occurrence in the program's
    // lifetime
    (name, () => Future.successful(CheckResult(name, success = true, JObject("status" -> JString("not initialized")))))
  }

  def ok(name: String): (String, CheckerFn) = {
    (name, () => Future.successful(CheckResult(name, success = true, JObject("status" -> JString("ok")))))
  }

  /** Gathers process data using Prometheus' collectors. Always succeeds. */
  def process(): (String, CheckerFn) = {
    ("process", { () =>
      val dataPoints = Collections.list(CollectorRegistry.defaultRegistry.filteredMetricFamilySamples(Set(
        "process_cpu_seconds_total",
        "process_start_time_seconds",
        "process_virtual_memory_bytes",
        "process_resident_memory_bytes",
        "jvm_threads_current"
      ).asJava)).asScala

      val json = dataPoints.foldRight(JObject())((samples, jo) => jo.merge(
        JObject(samples.name.replaceAll("_", "-") -> JDouble(samples.samples.asScala.last.value))
      )).merge(JObject(("status", JString("ok"))))

      Future.successful(CheckResult("process", success = true, json))
    })
  }

  /** Extracts relevant kafka metrics and evaluates whether they are healthy. */
  private def processKafkaMetrics(name: String, metrics: collection.Map[MetricName, Metric], connectionCountMustBeNonZero: Boolean) = {
    implicit class RichMetric(m: Metric) {
      // we're never interested in consumer node metrics, so let's get rid of them here
      @inline def is(name: String): Boolean = m.metricName().group() != "consumer-node-metrics" && m.metricName().name() == name

      @inline def toTuple: (String, AnyRef) = m.metricName().name() -> m.metricValue()
    }

    // TODO: ask for relevant metrics
    def relevantMetrics(metrics: collection.Map[MetricName, Metric]): Map[String, AnyRef] = {
      metrics.values.collect {
        case m if m is "last-heartbeat-seconds-ago" => m.toTuple
        case m if m is "heartbeat-total" => m.toTuple
        case m if m is "version" => m.toTuple
        case m if m is "request-rate" => m.toTuple
        case m if m is "response-rate" => m.toTuple
        case m if m is "commit-rate" => m.toTuple
        case m if m is "successful-authentication-total" => m.toTuple
        case m if m is "failed-authentication-total" => m.toTuple
        case m if m is "assigned-partitions" => m.toTuple
        case m if m.is("records-consumed-rate") && m.metricName().tags().size() == 1 => m.toTuple
        case m if m is "connection-count" => m.toTuple
        case m if m is "connection-close-total" => m.toTuple
        case m if m is "commit-latency-max" => m.toTuple
      }(collection.breakOut)
    }

    val processedMetrics = relevantMetrics(metrics)
    // TODO: ask for other failure conditions
    val success = {
      val isCountOk =
        !connectionCountMustBeNonZero || processedMetrics.getOrElse("connection-count", 0.0).asInstanceOf[Double] != 0.0
      if (!isCountOk)
        logger.warn(s"kafka client ({}) connection-count is zero, but it must be non-zero!",
          v("kafkaClientName", name))
      isCountOk
    } && {
      val lastHeartbeat = processedMetrics.getOrElse("last-heartbeat-seconds-ago", 0.0).asInstanceOf[Double]

      // the second part of that check is needed because kafka uses unreasonable defaults
      // (the value is roughly seconds since 1970, as of 2019-10-28)
      val isLastHeartbeatOk = lastHeartbeat < 60.0 || lastHeartbeat > 49 * 365 * 24 * 60 * 60

      if (!isLastHeartbeatOk)
        logger.warn("last kafka client ({}) heartbeat was {} seconds ago",
          v("kafkaClientName", name), v("heartbeatSecondsAgo", lastHeartbeat))

      isLastHeartbeatOk
    }

    def json(metrics: Map[String, AnyRef]): JValue =
      JsonMethods.fromJsonNode(JsonMethods.mapper.valueToTree[JsonNode](metrics.asJava))

    val payload = json(processedMetrics).merge(JObject("status" -> JString(if (success) "ok" else "nok")))

    CheckResult(name, success, payload)
  }

  /** Check kafka given a akka-kafka [[Control]] */
  def kafka(name: String, kafkaControl: Control, connectionCountMustBeNonZero: Boolean): (String, CheckerFn) = (name, { implicit ec =>
    kafkaControl.metrics.map(processKafkaMetrics(name, _, connectionCountMustBeNonZero))
  })

  /** Check kafka given a kafka [[Producer]] */
  def kafka(name: String, producer: Producer[_, _], connectionCountMustBeNonZero: Boolean): (String, CheckerFn) = (name, { () =>
    val metrics = producer.metrics().asScala
    Future.successful(processKafkaMetrics(name, metrics, connectionCountMustBeNonZero))
  })

  /** Check kafka given a kafka [[Consumer]] */
  def kafka(name: String, consumer: Consumer[_, _], connectionCountMustBeNonZero: Boolean): (String, CheckerFn) = (name, { () =>
    val metrics = consumer.metrics().asScala
    Future.successful(processKafkaMetrics(name, metrics, connectionCountMustBeNonZero))
  })

  /** Check if any kafka node is reachable */
  def kafkaNodesReachable(producer: Producer[_, _]): (String, HealthCheckServer.CheckerFn) = {
    val kafkaProducer = producer.asInstanceOf[KafkaProducer[_, _]]
    val f = classOf[KafkaProducer[_, _]].getDeclaredField("metadata")
    f.setAccessible(true)

    "kafka-nodes-reachable" -> { implicit ec =>
      val metadata = f.get(kafkaProducer).asInstanceOf[Metadata]

      Future.sequence(metadata.fetch().nodes().asScala.map { node =>
        Future {
          val kafkaNodeAddress = new InetSocketAddress(node.host(), node.port())

          // Q: Why isn't this just using [java.net.InetAddress.isReachable(int)]?
          // A: Because that uses ICMP and Kubernetes is sometimes weird about ICMP (so for example, ping doesn't work
          //    sometimes)
          val reachableViaKafkaTcpPort = try {
            val socket = new Socket()
            socket.connect(kafkaNodeAddress, 100)
            socket.close()
            true
          } catch {
            case _: Throwable => false
          }

          if (!reachableViaKafkaTcpPort)
            logger.warn("kafka node {} is not reachable", v("kafkaNodeAddress", kafkaNodeAddress))

          node.host() -> reachableViaKafkaTcpPort
        }
      }).map { reachability =>
        val success = reachability.exists(_._2)
        CheckResult("kafka-nodes-reachable", success,
          JObject(("status", if (success) JString("ok") else JString("nok")) ::
            reachability.toList.map { case (host, reachable) => host -> JBool(reachable) }))
      }
    }
  }
}

// below is udash-rest related stuff

@adjustSchema(HealthCheckResponse.flatten)
sealed trait HealthCheckResponse

case class HealthCheckSuccess(payload: JsonValue) extends HealthCheckResponse

object HealthCheckSuccess extends RestDataWrapperCompanion[JsonValue, HealthCheckSuccess] {
  implicit val schema: RestSchema[HealthCheckSuccess] = RestSchema.plain(Schema(`type` = DataType.Object))
}

case class HealthCheckFailure(payload: JsonValue) extends HealthCheckResponse

object HealthCheckFailure extends RestDataWrapperCompanion[JsonValue, HealthCheckFailure] {
  implicit val schema: RestSchema[HealthCheckFailure] = RestSchema.plain(Schema(`type` = DataType.Object))
}

object HealthCheckResponse extends RestDataCompanion[HealthCheckResponse] {
  // adds custom status codes
  implicit def asRestResp(implicit
    successAsRaw: AsRaw[HttpBody, HealthCheckSuccess],
    failureAsRaw: AsRaw[HttpBody, HealthCheckFailure]
  ): AsRaw[RestResponse, HealthCheckResponse] = {
    AsRaw.create {
      case s: HealthCheckSuccess => successAsRaw.asRaw(s).defaultResponse.recoverHttpError
      case f: HealthCheckFailure => failureAsRaw.asRaw(f).defaultResponse.copy(code = 500).recoverHttpError
    }
  }

  def flatten(s: Schema): Schema = {
    s.copy(oneOf = s.oneOf.map {
      case RefOr.Value(v) => v.properties.head._2
      case x => x
    })
  }
}

trait HealthCheckApi {
  @GET def live(): Future[HealthCheckResponse]

  @GET def ready(): Future[HealthCheckResponse]
}

object HealthCheckApi extends DefaultRestApiCompanion[HealthCheckApi]
