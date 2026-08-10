/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v1_1;

import io.opentelemetry.instrumentation.api.util.VirtualField;
import javax.annotation.Nullable;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

public final class JmsConsumerContext {

  private static final VirtualField<MessageConsumer, String> consumerSubscriptionNameField =
      VirtualField.find(MessageConsumer.class, String.class);
  private static final VirtualField<Message, String> messageSubscriptionNameField =
      VirtualField.find(Message.class, String.class);

  public static void setSubscriptionName(MessageConsumer consumer, String subscriptionName) {
    consumerSubscriptionNameField.set(consumer, subscriptionName);
  }

  public static void setSubscriptionName(Message message, @Nullable String subscriptionName) {
    messageSubscriptionNameField.set(message, subscriptionName);
  }

  @Nullable
  public static String getSubscriptionName(MessageConsumer consumer) {
    return consumerSubscriptionNameField.get(consumer);
  }

  @Nullable
  public static String getSubscriptionName(Message message) {
    return messageSubscriptionNameField.get(message);
  }

  @Nullable
  public static MessageListener wrapMessageListener(
      MessageConsumer consumer, @Nullable MessageListener messageListener) {
    if (messageListener == null) {
      return null;
    }
    String subscriptionName = consumerSubscriptionNameField.get(consumer);
    return subscriptionName == null
        ? messageListener
        : new SubscriptionMessageListener(messageListener, subscriptionName);
  }

  @Nullable
  public static MessageListener unwrapMessageListener(@Nullable MessageListener messageListener) {
    return messageListener instanceof SubscriptionMessageListener
        ? ((SubscriptionMessageListener) messageListener).delegate()
        : messageListener;
  }

  private JmsConsumerContext() {}
}
