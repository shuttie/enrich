/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.fs2.io

import cats.implicits._
import cats.Applicative
import cats.effect.{Blocker, ContextShift, Resource, Sync, Timer}

import fs2.Stream

import com.codahale.metrics.{Gauge, MetricRegistry, Slf4jReporter}

import org.slf4j.LoggerFactory

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.MetricsReporter

trait Metrics[F[_]] {

  /** Send latest metrics to reporter */
  def report: Stream[F, Unit]

  /**
   * Track latency between collector hit and enrichment
   * This function gets current timestamp by itself
   */
  def enrichLatency(collectorTstamp: Option[Long]): F[Unit]

  /** Increment raw payload count */
  def rawCount: F[Unit]

  /** Increment good enriched events */
  def goodCount: F[Unit]

  /** Increment bad events */
  def badCount: F[Unit]
}

object Metrics {

  val LoggerName = "enrich.metrics"
  val LatencyGaugeName = "latency"
  val RawCounterName = "raw"
  val GoodCounterName = "good"
  val BadCounterName = "bad"

  def build[F[_]: ContextShift: Sync: Timer](
    blocker: Blocker,
    config: MetricsReporter
  ): F[Metrics[F]] =
    for {
      registry <- Sync[F].delay((new MetricRegistry()))
      rep = reporter(blocker, config, registry)
    } yield ofRegistry(rep, registry)

  def reporter[F[_]: Sync: ContextShift: Timer](
    blocker: Blocker,
    config: MetricsReporter,
    registry: MetricRegistry
  ): Stream[F, Unit] =
    config match {
      case MetricsReporter.Stdout(period, prefix) =>
        for {
          logger <- Stream.eval(Sync[F].delay(LoggerFactory.getLogger(LoggerName)))
          reporter <-
            Stream.resource(
              Resource.fromAutoCloseable(
                Sync[F].delay(
                  Slf4jReporter.forRegistry(registry).outputTo(logger).prefixedWith(prefix.getOrElse(MetricsReporter.DefaultPrefix)).build
                )
              )
            )
          _ <- Stream.fixedDelay[F](period)
          _ <- Stream.eval(Sync[F].delay(reporter.report()))
        } yield ()
      case statsd: MetricsReporter.StatsD =>
        StatsDReporter.stream(blocker, statsd, registry)
    }

  private def ofRegistry[F[_]: Sync](reporter: Stream[F, Unit], registry: MetricRegistry): Metrics[F] =
    new Metrics[F] {
      val rawCounter = registry.counter(RawCounterName)
      val goodCounter = registry.counter(GoodCounterName)
      val badCounter = registry.counter(BadCounterName)

      def report: Stream[F, Unit] = reporter

      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] =
        collectorTstamp match {
          case Some(tstamp) =>
            Sync[F]
              .delay {
                registry.remove(LatencyGaugeName)
                val now = System.currentTimeMillis()
                val _ = registry.register(LatencyGaugeName, getGauge(now, tstamp))
              }
              .handleError {
                // Two threads can run into a race condition registering a gauge
                case _: IllegalArgumentException => ()
              }
          case None =>
            Sync[F].unit
        }

      def rawCount: F[Unit] =
        Sync[F].delay(rawCounter.inc())

      def goodCount: F[Unit] =
        Sync[F].delay(goodCounter.inc())

      def badCount: F[Unit] =
        Sync[F].delay(badCounter.inc())

      private def getGauge(now: Long, collectorTstamp: Long): Gauge[Long] =
        new Gauge[Long] {
          def getValue: Long = now - collectorTstamp
        }
    }

  def noop[F[_]: Applicative]: Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit] = Stream.empty.covary[F]
      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] = Applicative[F].unit
      def rawCount: F[Unit] = Applicative[F].unit
      def goodCount: F[Unit] = Applicative[F].unit
      def badCount: F[Unit] = Applicative[F].unit
    }
}
