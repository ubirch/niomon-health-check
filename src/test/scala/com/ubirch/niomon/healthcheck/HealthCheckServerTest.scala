package com.ubirch.niomon.healthcheck

import org.json4s.JsonAST.JObject
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Future

class HealthCheckServerTest extends FlatSpec with Matchers {
  "HealthCheckServer" should "work" ignore {
    val server = new HealthCheckServer(
      livenessChecks = Map("foo" -> { _ => Future.successful(CheckResult("foo", success = true, JObject())) }),
      readinessChecks = Map("bar" -> { _ => Future.successful(CheckResult("bar", success = false, JObject())) }),
      "http://localhost:8888/health"
    )
    server.run(8888)
    server.join()
  }
}
