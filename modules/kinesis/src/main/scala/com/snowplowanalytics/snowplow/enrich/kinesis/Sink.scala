/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.nio.ByteBuffer
import java.util.UUID
import scala.collection.JavaConverters._

import cats.implicits._

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync, Timer}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import retry.syntax.all._
import retry.RetryPolicies._

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Monitoring, Output}

object Sink {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    output: Output,
    monitoring: Monitoring
  ): Resource[F, ByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            for {
              producer <- Resource.pure[F, AmazonKinesis](mkProducer(o, region))
              _ <- Resource.pure[F, Monitoring](monitoring)
            } yield records => records.grouped(o.collection.maxCount.toInt).toList.traverse_(g => writeToKinesis(blocker, o, producer, toKinesisRecords(g.map(AttributedData(_, Map.empty)))))
          case None =>
            Resource.eval(Sync[F].raiseError(new RuntimeException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  def initAttributed[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    output: Output,
    monitoring: Monitoring
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            for {
              producer <- Resource.pure[F, AmazonKinesis](mkProducer(o, region))
              _ <- Resource.pure[F, Monitoring](monitoring)
            } yield records => records.grouped(o.collection.maxCount.toInt).toList.traverse_(g => writeToKinesis(blocker, o, producer, toKinesisRecords(g)))
          case None =>
            Resource.eval(Sync[F].raiseError(new RuntimeException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  private def mkProducer(
    config: Output.Kinesis,
    region: String
  ): AmazonKinesis = {
    val endpoint = config.customEndpoint.map(_.toString).getOrElse(region match {
      case cn @ "cn-north-1" => s"https://kinesis.$cn.amazonaws.com.cn"
      case _ => s"https://kinesis.$region.amazonaws.com"
    })

    val endpointConfiguration = new EndpointConfiguration(endpoint, region)
    
    val provider = new DefaultAWSCredentialsProviderChain()

    AmazonKinesisClientBuilder
      .standard()
      .withCredentials(provider)
      .withEndpointConfiguration(endpointConfiguration)
      .build()
  }

  private def writeToKinesis[F[_]: ContextShift: Sync: Timer](
    blocker: Blocker,
    config: Output.Kinesis,
    kinesis: AmazonKinesis,
    records: List[KinesisRecord]
  ): F[Unit] = {
    val retryPolicy = capDelay[F](config.backoffPolicy.maxBackoff, fullJitter[F](config.backoffPolicy.minBackoff))
      .join(limitRetries(config.backoffPolicy.maxRetries))
  
    val withoutRetry = blocker.blockOn(Sync[F].delay(putRecords(kinesis, config.streamName, records)))

    val withRetry = 
      withoutRetry.retryingOnAllErrors(
        policy = retryPolicy,
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(exception)(s"Writing to Kinesis errored (${retryDetails.retriesSoFar} retries from cats-retry)")
      )
    
    for {
      _ <- Logger[F].info(s"Writing ${records.size} records to ${config.streamName}")
      putRecordsResult <- withRetry
      failuresRetried <- 
        if(putRecordsResult.getFailedRecordCount == 0)
          Logger[F].info(s"Successfully wrote ${records.size} records to ${config.streamName}")
        else {
          val failurePairs = records.zip(putRecordsResult.getRecords.asScala).filter(_._2.getErrorMessage != null)
          val (failedRecords, failedResults) = failurePairs.unzip
          logErrorsSummary(getErrorsSummary(failedResults)) *> writeToKinesis(blocker, config, kinesis, failedRecords)
        }
    } yield failuresRetried
  }

  private def toKinesisRecords(records: List[AttributedData[Array[Byte]]]): List[KinesisRecord] =
    records.map { r =>
      val partitionKey =
        r.attributes.toList match { // there can be only one attribute : the partition key
          case head :: Nil => head._2
          case _ => UUID.randomUUID().toString
        }
      val data = ByteBuffer.wrap(r.data)
      KinesisRecord(data, partitionKey)
    }

  private def putRecords(
    kinesis: AmazonKinesis,
    streamName: String,
    records: List[KinesisRecord]
  ): PutRecordsResult = {
    val putRecordsRequest = {
      val prr = new PutRecordsRequest()
      prr.setStreamName(streamName)
      val putRecordsRequestEntryList = records.map { r =>
        val prre = new PutRecordsRequestEntry()
        prre.setPartitionKey(r.partitionKey)
        prre.setData(r.data)
        prre
      }
      prr.setRecords(putRecordsRequestEntryList.asJava)
      prr
    }
    kinesis.putRecords(putRecordsRequest)
  }

  private def getErrorsSummary(badResponses: List[PutRecordsResultEntry]): Map[String, (Long, String)] =
    badResponses.foldLeft(Map[String, (Long, String)]())((counts, r) =>
      if (counts.contains(r.getErrorCode))
        counts + (r.getErrorCode -> (counts(r.getErrorCode)._1 + 1 -> r.getErrorMessage))
      else
        counts + (r.getErrorCode -> ((1, r.getErrorMessage)))
    )

  private def logErrorsSummary[F[_]: Sync](errorsSummary: Map[String, (Long, String)]): F[Unit] =
    errorsSummary.map { case (errorCode, (count, sampleMessage)) =>
      Logger[F].error(
        s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}"
      )
    }
      .toList
      .sequence_

  private case class KinesisRecord(data: ByteBuffer, partitionKey: String)
}
