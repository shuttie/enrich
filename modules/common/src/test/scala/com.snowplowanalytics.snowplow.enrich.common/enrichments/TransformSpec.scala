package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.syntax.option._
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers.MapOps
import org.specs2.mutable.Specification

class TransformSpec extends Specification {
  "transform should not drop properties with explicit null values" >> {
    val ApplicationJsonWithCapitalCharset = "application/json; charset=UTF-8"
    val raw = RawEvent(
      CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2"),
      Map(
        "tv" -> "0",
        "p" -> "web",
        "e" -> "pv",
        "nuid" -> "123",
        "cx" -> "ewogICJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvY29udGV4dHMvanNvbnNjaGVtYS8xLTAtMCIsCiAgImRhdGEiOiBbCiAgICB7CiAgICAgICJzY2hlbWEiOiAiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvY2xpZW50X3Nlc3Npb24vanNvbnNjaGVtYS8xLTAtMSIsCiAgICAgICJkYXRhIjogewogICAgICAgICJzZXNzaW9uSW5kZXgiOiAxLAogICAgICAgICJzdG9yYWdlTWVjaGFuaXNtIjogIkxPQ0FMX1NUT1JBR0UiLAogICAgICAgICJmaXJzdEV2ZW50SWQiOiAiNWMzM2ZjY2YtNmJlNS00Y2U2LWFmYjEtZTM0MDI2YTNjYTc1IiwKICAgICAgICAic2Vzc2lvbklkIjogIjIxYzJhMGRkLTg5MmQtNDJkMS1iMTU2LTNhOWQ0ZTE0N2VlZiIsCiAgICAgICAgInByZXZpb3VzU2Vzc2lvbklkIjogbnVsbCwKICAgICAgICAidXNlcklkIjogIjIwZDYzMWI4LTc4MzctNDlkZi1hNzNlLTZkYTczMTU0ZTZmZCIKICAgICAgfQogICAgfQogIF0KfQ=="
      ).toOpt,
      ApplicationJsonWithCapitalCharset.some,
      CollectorPayload.Source("source", "UTF-8", None),
      CollectorPayload.Context(None, None, None, None, Nil, None)
    )

    val enriched = new EnrichedEvent
    Transform.transform(raw, enriched)

    enriched.contexts mustEqual """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1","data":{"sessionIndex":1,"storageMechanism":"LOCAL_STORAGE","firstEventId":"5c33fccf-6be5-4ce6-afb1-e34026a3ca75","sessionId":"21c2a0dd-892d-42d1-b156-3a9d4e147eef","previousSessionId":null,"userId":"20d631b8-7837-49df-a73e-6da73154e6fd"}}]}"""
  }
}
