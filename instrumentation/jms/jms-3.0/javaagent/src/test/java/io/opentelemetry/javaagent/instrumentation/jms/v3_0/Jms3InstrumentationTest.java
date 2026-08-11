/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v3_0;

import static io.opentelemetry.api.trace.SpanKind.CLIENT;
import static io.opentelemetry.api.trace.SpanKind.CONSUMER;
import static io.opentelemetry.api.trace.SpanKind.PRODUCER;
import static io.opentelemetry.instrumentation.api.internal.SemconvStability.emitStableMessagingSemconv;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.equalTo;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_MESSAGE_ID;
import static io.opentelemetry.semconv.incubating.MessagingIncubatingAttributes.MESSAGING_SYSTEM;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class Jms3InstrumentationTest extends AbstractJms3Test {

  @SuppressWarnings("deprecation") // using deprecated semconv
  @Test
  void capturesDurableConsumerName() throws Exception {
    Topic topic = session.createTopic("durable-topic");
    TextMessage sentMessage = session.createTextMessage("hello there");
    MessageProducer producer = session.createProducer(topic);
    cleanup.deferCleanup(producer);
    MessageConsumer consumer = session.createDurableConsumer(topic, "durable-subscription");
    cleanup.deferCleanup(consumer);

    testing.runWithSpan("producer parent", () -> producer.send(sentMessage));
    CompletableFuture<TextMessage> receivedMessage = new CompletableFuture<>();
    MessageListener listener = message -> receivedMessage.complete((TextMessage) message);
    consumer.setMessageListener(listener);
    assertThat(consumer.getMessageListener()).isSameAs(listener);

    String messageId = receivedMessage.get(10, SECONDS).getJMSMessageID();
    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("producer parent").hasNoParent(),
                span ->
                    span.hasKind(PRODUCER)
                        .hasParent(trace.getSpan(0))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName("durable-topic", "durable-topic"),
                            oldOperation("publish"),
                            operationName("send"),
                            operationType("send"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(false)),
                span ->
                    span.hasKind(CONSUMER)
                        .hasParent(trace.getSpan(1))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName("durable-topic", "durable-topic"),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            messagingTempDestination(false),
                            subscriptionName("durable-subscription"))));
  }

  @ParameterizedTest
  @MethodSource("sharedConsumerArguments")
  void capturesSharedConsumerNameOnReceive(
      String subscriptionName, SharedConsumerFactory consumerFactory) throws JMSException {
    Topic topic = session.createTopic("shared-receive-topic");
    TextMessage sentMessage = session.createTextMessage("hello there");
    MessageProducer producer = session.createProducer(topic);
    cleanup.deferCleanup(producer);
    MessageConsumer consumer = consumerFactory.create(session, topic, subscriptionName);
    cleanup.deferCleanup(consumer);

    testing.runWithSpan("producer parent", () -> producer.send(sentMessage));
    TextMessage receivedMessage =
        testing.runWithSpan("consumer parent", () -> (TextMessage) consumer.receive());

    String messageId = receivedMessage.getJMSMessageID();
    AtomicReference<SpanData> producerSpan = new AtomicReference<>();
    testing.waitAndAssertTraces(
        trace -> {
          trace.hasSpansSatisfyingExactly(
              span -> span.hasName("producer parent").hasNoParent(),
              span ->
                  span.hasKind(PRODUCER)
                      .hasParent(trace.getSpan(0))
                      .hasAttributesSatisfyingExactly(
                          equalTo(MESSAGING_SYSTEM, "jms"),
                          messagingDestinationName("shared-receive-topic", "shared-receive-topic"),
                          oldOperation("publish"),
                          operationName("send"),
                          operationType("send"),
                          equalTo(MESSAGING_MESSAGE_ID, messageId),
                          messagingTempDestination(false)));
          producerSpan.set(trace.getSpan(1));
        },
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("consumer parent").hasNoParent(),
                span ->
                    span.hasKind(emitStableMessagingSemconv() ? CLIENT : CONSUMER)
                        .hasParent(trace.getSpan(0))
                        .hasLinks(LinkData.create(producerSpan.get().getSpanContext()))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(
                                "shared-receive-topic", "shared-receive-topic"),
                            oldOperation("receive"),
                            operationName("receive"),
                            operationType("receive"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId),
                            subscriptionName(subscriptionName))));
  }

  @ParameterizedTest
  @MethodSource("sharedConsumerArguments")
  void capturesSharedConsumerNameOnProviderStyleListenerDispatch(
      String subscriptionName, SharedConsumerFactory consumerFactory) throws JMSException {
    Topic topic = session.createTopic("shared-listener-topic");
    TextMessage message = session.createTextMessage("hello there");
    message.setJMSDestination(topic);
    MessageConsumer consumer = consumerFactory.create(session, topic, subscriptionName);
    cleanup.deferCleanup(consumer);
    MessageListener listener = ignored -> {};
    consumer.setMessageListener(listener);

    MessageListener providerListener = consumer.getMessageListener();
    assertThat(providerListener).isSameAs(listener);
    providerListener.onMessage(message);

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? "process shared-listener-topic"
                                : "shared-listener-topic process")
                        .hasKind(CONSUMER)
                        .hasNoParent()
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(
                                "shared-listener-topic", "shared-listener-topic"),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            messagingTempDestination(false),
                            subscriptionName(subscriptionName))));
  }

  @Test
  void capturesSharedConsumerNameAfterImplicitConnectionClose() throws JMSException {
    MessageListener listener = ignored -> {};
    Connection firstConnection = connectionFactory.createConnection();
    firstConnection.setClientID("implicit-close-first");
    firstConnection.start();
    Session firstSession = firstConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic firstTopic = firstSession.createTopic("implicit-close-topic");
    MessageConsumer firstConsumer = firstSession.createConsumer(firstTopic);
    firstConsumer.setMessageListener(listener);
    firstConnection.close();

    Connection secondConnection = connectionFactory.createConnection();
    cleanup.deferCleanup(secondConnection);
    secondConnection.setClientID("implicit-close-second");
    secondConnection.start();
    Session secondSession = secondConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic secondTopic = secondSession.createTopic("implicit-close-topic");
    TextMessage message = secondSession.createTextMessage("hello there");
    message.setJMSDestination(secondTopic);
    MessageConsumer secondConsumer =
        secondSession.createSharedConsumer(secondTopic, "active-subscription");
    secondConsumer.setMessageListener(listener);

    secondConsumer.getMessageListener().onMessage(message);

    testing.waitAndAssertTraces(
        trace ->
            trace.hasSpansSatisfyingExactly(
                span ->
                    span.hasKind(CONSUMER)
                        .hasNoParent()
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(
                                "implicit-close-topic", "implicit-close-topic"),
                            oldOperation("process"),
                            operationName("process"),
                            operationType("process"),
                            messagingTempDestination(false),
                            subscriptionName("active-subscription"))));
  }

  @SuppressWarnings("deprecation") // using deprecated semconv
  @ParameterizedTest
  @MethodSource("destinationArguments")
  void testMessageConsumer(DestinationFactory destinationFactory, boolean isTemporary)
      throws JMSException {

    // given
    Destination destination = destinationFactory.create(session);
    TextMessage sentMessage = session.createTextMessage("hello there");

    MessageProducer producer = session.createProducer(destination);
    cleanup.deferCleanup(producer);
    MessageConsumer consumer = session.createConsumer(destination);
    cleanup.deferCleanup(consumer);

    // when
    testing.runWithSpan("producer parent", () -> producer.send(sentMessage));

    TextMessage receivedMessage =
        testing.runWithSpan("consumer parent", () -> (TextMessage) consumer.receive());

    // then
    assertThat(receivedMessage.getText()).isEqualTo(sentMessage.getText());

    String actualDestinationName = ((ActiveMQDestination) destination).getName();
    // artemis consumers don't know whether the destination is temporary or not
    String producerDestinationName = isTemporary ? "(temporary)" : actualDestinationName;
    String messageId = receivedMessage.getJMSMessageID();

    AtomicReference<SpanData> producerSpan = new AtomicReference<>();
    testing.waitAndAssertTraces(
        trace -> {
          trace.hasSpansSatisfyingExactly(
              span -> span.hasName("producer parent").hasNoParent(),
              span ->
                  span.hasName(
                          emitStableMessagingSemconv()
                              ? producerDestinationName.equals("(temporary)")
                                  ? "send"
                                  : "send " + producerDestinationName
                              : producerDestinationName + " publish")
                      .hasKind(PRODUCER)
                      .hasParent(trace.getSpan(0))
                      .hasAttributesSatisfyingExactly(
                          equalTo(MESSAGING_SYSTEM, "jms"),
                          messagingDestinationName(producerDestinationName, actualDestinationName),
                          oldOperation("publish"),
                          operationName("send"),
                          operationType("send"),
                          equalTo(MESSAGING_MESSAGE_ID, messageId),
                          messagingTempDestination(isTemporary)));

          producerSpan.set(trace.getSpan(1));
        },
        trace ->
            trace.hasSpansSatisfyingExactly(
                span -> span.hasName("consumer parent").hasNoParent(),
                span ->
                    span.hasName(
                            emitStableMessagingSemconv()
                                ? actualDestinationName.equals("(temporary)")
                                    ? "receive"
                                    : "receive " + actualDestinationName
                                : actualDestinationName + " receive")
                        .hasKind(emitStableMessagingSemconv() ? CLIENT : CONSUMER)
                        .hasParent(trace.getSpan(0))
                        .hasLinks(LinkData.create(producerSpan.get().getSpanContext()))
                        .hasAttributesSatisfyingExactly(
                            equalTo(MESSAGING_SYSTEM, "jms"),
                            messagingDestinationName(actualDestinationName, actualDestinationName),
                            oldOperation("receive"),
                            operationName("receive"),
                            operationType("receive"),
                            equalTo(MESSAGING_MESSAGE_ID, messageId))));
  }

  private static Stream<Arguments> sharedConsumerArguments() {
    return Stream.of(
        argumentSet(
            "shared", "shared-subscription", (SharedConsumerFactory) Session::createSharedConsumer),
        argumentSet(
            "shared durable",
            "shared-durable-subscription",
            (SharedConsumerFactory) Session::createSharedDurableConsumer));
  }

  @FunctionalInterface
  interface SharedConsumerFactory {

    MessageConsumer create(Session session, Topic topic, String subscriptionName)
        throws JMSException;
  }
}
