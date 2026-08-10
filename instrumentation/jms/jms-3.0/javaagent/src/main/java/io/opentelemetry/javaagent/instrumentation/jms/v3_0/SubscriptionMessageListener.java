/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v3_0;

import jakarta.jms.Message;
import jakarta.jms.MessageListener;

final class SubscriptionMessageListener implements MessageListener {

  private final MessageListener delegate;
  private final String subscriptionName;

  SubscriptionMessageListener(MessageListener delegate, String subscriptionName) {
    this.delegate = delegate;
    this.subscriptionName = subscriptionName;
  }

  @Override
  public void onMessage(Message message) {
    JmsConsumerContext.setSubscriptionName(message, subscriptionName);
    delegate.onMessage(message);
  }

  MessageListener delegate() {
    return delegate;
  }
}
