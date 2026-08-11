/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.pulsar.v2_8.telemetry;

import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_PARTITION_ID;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_MESSAGE_ID;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.SpanLinksExtractor;
import io.opentelemetry.instrumentation.api.internal.PropagatorBasedSpanLinksExtractor;
import java.util.Objects;
import org.apache.pulsar.client.api.Message;

final class PulsarBatchRequestSpanLinksExtractor implements SpanLinksExtractor<PulsarBatchRequest> {
  private static final PulsarMessagingAttributesGetter messagingAttributesGetter =
      new PulsarMessagingAttributesGetter();
  private static final PulsarBatchMessagingAttributesGetter batchMessagingAttributesGetter =
      new PulsarBatchMessagingAttributesGetter();

  private final TextMapPropagator propagator;
  private final SpanLinksExtractor<PulsarRequest> singleRecordLinkExtractor;

  PulsarBatchRequestSpanLinksExtractor(TextMapPropagator propagator) {
    this.propagator = propagator;
    this.singleRecordLinkExtractor =
        new PropagatorBasedSpanLinksExtractor<>(propagator, MessageTextMapGetter.INSTANCE);
  }

  @Override
  public void extract(
      SpanLinksBuilder spanLinks, Context parentContext, PulsarBatchRequest request) {

    boolean hasVaryingDestinations =
        emitStableMessagingSemconv() && hasVaryingDestinations(request);
    for (Message<?> message : request.getMessages()) {
      PulsarRequest messageRequest =
          PulsarRequest.create(message, request.getUrlData(), request.getSubscription());
      if (!emitStableMessagingSemconv()) {
        singleRecordLinkExtractor.extract(spanLinks, parentContext, messageRequest);
        continue;
      }

      Context extracted =
          propagator.extract(Context.root(), messageRequest, MessageTextMapGetter.INSTANCE);
      spanLinks.addLink(
          Span.fromContext(extracted).getSpanContext(),
          getLinkAttributes(request, messageRequest, hasVaryingDestinations));
    }
  }

  private static boolean hasVaryingDestinations(PulsarBatchRequest request) {
    String destination = null;
    boolean first = true;
    for (Message<?> message : request.getMessages()) {
      if (first) {
        destination = message.getTopicName();
        first = false;
      } else if (!Objects.equals(destination, message.getTopicName())) {
        return true;
      }
    }
    return false;
  }

  private static Attributes getLinkAttributes(
      PulsarBatchRequest batchRequest,
      PulsarRequest messageRequest,
      boolean hasVaryingDestinations) {
    AttributesBuilder attributes = Attributes.builder();
    attributes.put(
        MESSAGING_MESSAGE_ID, messagingAttributesGetter.getMessageId(messageRequest, null));

    String destination = messagingAttributesGetter.getDestination(messageRequest);
    if (hasVaryingDestinations
        || !Objects.equals(
            destination, batchMessagingAttributesGetter.getDestination(batchRequest))) {
      attributes.put(MESSAGING_DESTINATION_NAME, destination);
    }

    String partitionId = messagingAttributesGetter.getDestinationPartitionId(messageRequest);
    if (!Objects.equals(
        partitionId, batchMessagingAttributesGetter.getDestinationPartitionId(batchRequest))) {
      attributes.put(MESSAGING_DESTINATION_PARTITION_ID, partitionId);
    }
    return attributes.build();
  }
}
