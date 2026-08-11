/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.jms.v3_0;

import io.opentelemetry.instrumentation.api.util.VirtualField;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import javax.annotation.Nullable;

public final class JmsConsumerContext {

  private static final VirtualField<MessageConsumer, String> consumerSubscriptionNameField =
      VirtualField.find(MessageConsumer.class, String.class);
  private static final VirtualField<Message, String> messageSubscriptionNameField =
      VirtualField.find(Message.class, String.class);
  private static final VirtualField<MessageConsumer, ConsumerListenerRegistration>
      consumerListenerRegistrationField =
          VirtualField.find(MessageConsumer.class, ConsumerListenerRegistration.class);
  private static final VirtualField<MessageListener, ListenerSubscriptions>
      listenerSubscriptionsField =
          VirtualField.find(MessageListener.class, ListenerSubscriptions.class);
  private static final VirtualField<Session, ConsumerRegistry> sessionConsumersField =
      VirtualField.find(Session.class, ConsumerRegistry.class);
  private static final VirtualField<Connection, SessionRegistry> connectionSessionsField =
      VirtualField.find(Connection.class, SessionRegistry.class);

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
  public static String getSubscriptionName(MessageListener messageListener) {
    ListenerSubscriptions subscriptions = listenerSubscriptionsField.get(messageListener);
    return subscriptions == null ? null : subscriptions.getSubscriptionName();
  }

  public static void registerConsumer(Session session, MessageConsumer consumer) {
    getOrCreateConsumerRegistry(session).add(consumer);
  }

  public static void registerSession(Connection connection, Session session) {
    getOrCreateSessionRegistry(connection).add(session);
  }

  public static void closeConnection(Connection connection) {
    SessionRegistry sessions = connectionSessionsField.get(connection);
    if (sessions != null) {
      sessions.close();
    }
  }

  public static void closeSession(Session session) {
    ConsumerRegistry consumers = sessionConsumersField.get(session);
    if (consumers != null) {
      consumers.close();
    }
  }

  @Nullable
  public static ListenerUpdate updateMessageListener(
      MessageConsumer consumer, @Nullable MessageListener messageListener) {
    ConsumerListenerRegistration registration = getOrCreateConsumerListenerRegistration(consumer);
    synchronized (registration) {
      if (registration.closed || registration.messageListener == messageListener) {
        return null;
      }
      MessageListener previousMessageListener = registration.messageListener;
      replaceMessageListener(registration, consumer, messageListener);
      return new ListenerUpdate(registration, previousMessageListener, registration.version);
    }
  }

  public static void rollbackMessageListener(
      MessageConsumer consumer, @Nullable ListenerUpdate update) {
    if (update == null) {
      return;
    }
    ConsumerListenerRegistration registration = consumerListenerRegistrationField.get(consumer);
    if (registration != update.registration) {
      return;
    }
    synchronized (registration) {
      if (!registration.closed && registration.version == update.version) {
        replaceMessageListener(registration, consumer, update.previousMessageListener);
      }
    }
  }

  public static void closeConsumer(MessageConsumer consumer) {
    ConsumerListenerRegistration registration = consumerListenerRegistrationField.get(consumer);
    if (registration == null) {
      return;
    }
    synchronized (registration) {
      if (!registration.closed) {
        replaceMessageListener(registration, consumer, null);
        registration.closed = true;
      }
    }
  }

  private static void replaceMessageListener(
      ConsumerListenerRegistration registration,
      MessageConsumer consumer,
      @Nullable MessageListener messageListener) {
    if (registration.messageListener != null) {
      removeSubscription(registration.messageListener, consumer);
    }
    registration.messageListener = messageListener;
    registration.version++;
    if (messageListener != null) {
      addSubscription(messageListener, consumer, consumerSubscriptionNameField.get(consumer));
    }
  }

  private static ConsumerListenerRegistration getOrCreateConsumerListenerRegistration(
      MessageConsumer consumer) {
    ConsumerListenerRegistration registration = consumerListenerRegistrationField.get(consumer);
    if (registration == null) {
      synchronized (consumerListenerRegistrationField) {
        registration = consumerListenerRegistrationField.get(consumer);
        if (registration == null) {
          registration = new ConsumerListenerRegistration();
          consumerListenerRegistrationField.set(consumer, registration);
        }
      }
    }
    return registration;
  }

  private static ConsumerRegistry getOrCreateConsumerRegistry(Session session) {
    ConsumerRegistry registry = sessionConsumersField.get(session);
    if (registry == null) {
      synchronized (sessionConsumersField) {
        registry = sessionConsumersField.get(session);
        if (registry == null) {
          registry = new ConsumerRegistry();
          sessionConsumersField.set(session, registry);
        }
      }
    }
    return registry;
  }

  private static SessionRegistry getOrCreateSessionRegistry(Connection connection) {
    SessionRegistry registry = connectionSessionsField.get(connection);
    if (registry == null) {
      synchronized (connectionSessionsField) {
        registry = connectionSessionsField.get(connection);
        if (registry == null) {
          registry = new SessionRegistry();
          connectionSessionsField.set(connection, registry);
        }
      }
    }
    return registry;
  }

  private static void addSubscription(
      MessageListener messageListener,
      MessageConsumer consumer,
      @Nullable String subscriptionName) {
    ListenerSubscriptions subscriptions = listenerSubscriptionsField.get(messageListener);
    if (subscriptions == null) {
      synchronized (listenerSubscriptionsField) {
        subscriptions = listenerSubscriptionsField.get(messageListener);
        if (subscriptions == null) {
          subscriptions = new ListenerSubscriptions();
          listenerSubscriptionsField.set(messageListener, subscriptions);
        }
      }
    }
    subscriptions.add(consumer, subscriptionName);
  }

  private static void removeSubscription(
      MessageListener messageListener, MessageConsumer consumer) {
    ListenerSubscriptions subscriptions = listenerSubscriptionsField.get(messageListener);
    if (subscriptions != null) {
      subscriptions.remove(consumer);
    }
  }

  private JmsConsumerContext() {}

  public static class ListenerUpdate {
    private final ConsumerListenerRegistration registration;
    private final MessageListener previousMessageListener;
    private final long version;

    private ListenerUpdate(
        ConsumerListenerRegistration registration,
        @Nullable MessageListener previousMessageListener,
        long version) {
      this.registration = registration;
      this.previousMessageListener = previousMessageListener;
      this.version = version;
    }
  }

  private static class ConsumerListenerRegistration {
    @Nullable private MessageListener messageListener;
    private long version;
    private boolean closed;
  }

  private static class ListenerSubscriptions {
    private final Map<MessageConsumer, String> subscriptions = new WeakHashMap<>();

    synchronized void add(MessageConsumer consumer, @Nullable String subscriptionName) {
      subscriptions.put(consumer, subscriptionName);
    }

    synchronized void remove(MessageConsumer consumer) {
      subscriptions.remove(consumer);
    }

    @Nullable
    synchronized String getSubscriptionName() {
      boolean found = false;
      String subscriptionName = null;
      for (String candidate : subscriptions.values()) {
        if (found && !Objects.equals(subscriptionName, candidate)) {
          return null;
        }
        found = true;
        subscriptionName = candidate;
      }
      return subscriptionName;
    }
  }

  private static class ConsumerRegistry {
    private final Map<MessageConsumer, Boolean> consumers = new WeakHashMap<>();
    private boolean closed;

    synchronized void add(MessageConsumer consumer) {
      if (closed) {
        closeConsumer(consumer);
      } else {
        consumers.put(consumer, true);
      }
    }

    synchronized void close() {
      closed = true;
      for (MessageConsumer consumer : new ArrayList<>(consumers.keySet())) {
        closeConsumer(consumer);
      }
      consumers.clear();
    }
  }

  private static class SessionRegistry {
    private final Map<Session, Boolean> sessions = new WeakHashMap<>();
    private boolean closed;

    synchronized void add(Session session) {
      if (closed) {
        closeSession(session);
      } else {
        sessions.put(session, true);
      }
    }

    synchronized void close() {
      closed = true;
      for (Session session : new ArrayList<>(sessions.keySet())) {
        closeSession(session);
      }
      sessions.clear();
    }
  }
}
