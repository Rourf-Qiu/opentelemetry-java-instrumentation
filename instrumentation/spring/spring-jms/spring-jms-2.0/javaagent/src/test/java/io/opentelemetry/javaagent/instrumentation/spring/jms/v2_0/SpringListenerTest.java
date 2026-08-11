/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.spring.jms.v2_0;

import static io.opentelemetry.api.trace.SpanKind.CLIENT;
import static io.opentelemetry.api.trace.SpanKind.CONSUMER;
import static io.opentelemetry.api.trace.SpanKind.PRODUCER;
import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertCounter;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertHistogram;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoDeprecatedMetrics;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoMetric;
import static io.opentelemetry.instrumentation.testing.junit.MessagingMetricsAssertions.assertNoStableMetrics;
import static io.opentelemetry.instrumentation.testing.util.TelemetryDataUtil.orderByRootSpanKind;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_DESTINATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_NAME;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_OPERATION_TYPE;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.instrumentation.spring.jms.v2_0.AbstractJmsTest;
import io.opentelemetry.instrumentation.testing.internal.AutoCleanupExtension;
import io.opentelemetry.instrumentation.testing.junit.AgentInstrumentationExtension;
import io.opentelemetry.instrumentation.testing.junit.InstrumentationExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.ConnectionFactory;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jms.core.JmsTemplate;

class SpringListenerTest extends AbstractJmsTest {

  @RegisterExtension
  private static final InstrumentationExtension testing = AgentInstrumentationExtension.create();

  @RegisterExtension
  private static final AutoCleanupExtension cleanup = AutoCleanupExtension.create();

  @ParameterizedTest
  @ValueSource(classes = {AnnotatedListenerConfig.class, ManualListenerConfig.class})
  void receivingMessageInSpringListenerGeneratesSpans(Class<? extends AbstractConfig> config) {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(config);
    cleanup.deferCleanup(context);
    ConnectionFactory factory = context.getBean(ConnectionFactory.class);
    JmsTemplate template = new JmsTemplate(factory);

    template.convertAndSend("SpringListenerJms2", "a message");

    AtomicReference<SpanData> producerSpan = new AtomicReference<>();
    testing.waitAndAssertSortedTraces(
        orderByRootSpanKind(PRODUCER, emitStableMessagingSemconv() ? CLIENT : CONSUMER),
        trace -> {
          trace.hasSpansSatisfyingExactly(
              span -> assertProducerSpan(span, "SpringListenerJms2", false));
          producerSpan.set(trace.getSpan(0));
        },
        trace ->
            trace.hasSpansSatisfyingExactly(
                span ->
                    assertConsumerSpan(
                        span,
                        producerSpan.get(),
                        null,
                        "SpringListenerJms2",
                        "receive",
                        false,
                        null),
                span ->
                    assertConsumerSpan(
                        span,
                        producerSpan.get(),
                        trace.getSpan(0),
                        "SpringListenerJms2",
                        "process",
                        false,
                        null)));
    assertMetrics(testing, true);
  }

  static void assertMetrics(InstrumentationExtension testing, boolean receiveTelemetryEnabled) {
    if (!emitStableMessagingSemconv()) {
      assertNoStableMetrics(testing);
      assertNoDeprecatedMetrics(testing);
      return;
    }

    Attributes sendAttributes = metricAttributes("send", false);
    Attributes receiveAttributes = metricAttributes("receive", false);
    Attributes processAttributes = metricAttributes("process", false);
    assertCounter(
        testing, "io.opentelemetry.jms-1.1", "messaging.client.sent.messages", sendAttributes);
    assertHistogram(
        testing,
        "io.opentelemetry.spring-jms-2.0",
        "messaging.process.duration",
        processAttributes);
    if (receiveTelemetryEnabled) {
      assertCounter(
          testing,
          "io.opentelemetry.jms-1.1",
          "messaging.client.consumed.messages",
          receiveAttributes);
      assertHistogram(
          testing,
          "io.opentelemetry.jms-1.1",
          "messaging.client.operation.duration",
          metricAttributes("send", true),
          metricAttributes("receive", true));
      assertNoMetric(
          testing, "io.opentelemetry.spring-jms-2.0", "messaging.client.consumed.messages");
    } else {
      assertCounter(
          testing,
          "io.opentelemetry.spring-jms-2.0",
          "messaging.client.consumed.messages",
          processAttributes);
      assertHistogram(
          testing,
          "io.opentelemetry.jms-1.1",
          "messaging.client.operation.duration",
          metricAttributes("send", true));
      assertNoMetric(testing, "io.opentelemetry.jms-1.1", "messaging.client.consumed.messages");
    }
    assertNoDeprecatedMetrics(testing);
  }

  private static Attributes metricAttributes(String operation, boolean includeOperationType) {
    AttributesBuilder builder =
        Attributes.builder()
            .put(MESSAGING_OPERATION_NAME, operation)
            .put(MESSAGING_SYSTEM, "jms")
            .put(MESSAGING_DESTINATION_NAME, "SpringListenerJms2");
    if (includeOperationType) {
      builder.put(MESSAGING_OPERATION_TYPE, operation);
    }
    return builder.build();
  }
}
