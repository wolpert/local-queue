package com.codeheadsystems.queue.impl;

import com.codeheadsystems.metrics.Metrics;
import com.codeheadsystems.metrics.Tags;
import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.Queue;
import com.codeheadsystems.queue.QueueConfiguration;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.factory.QueueConfigurationFactory;
import com.codeheadsystems.queue.manager.MessageManager;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Queue.
 */
@Singleton
public class QueueImpl implements Queue {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueImpl.class);

  private final MessageManager messageManager;
  private final QueueConfiguration queueConfiguration;
  private final Metrics metrics;

  /**
   * Instantiates a new Queue.
   *
   * @param messageManager            the message manager
   * @param queueConfigurationFactory the queue configuration factory
   * @param metrics                   the metrics
   */
  @Inject
  public QueueImpl(final MessageManager messageManager,
                   final QueueConfigurationFactory queueConfigurationFactory,
                   final Metrics metrics) {
    this.messageManager = messageManager;
    this.queueConfiguration = queueConfigurationFactory.queueConfiguration();
    this.metrics = metrics;
    LOGGER.info("QueueImpl({}, {})", queueConfiguration, messageManager);
  }

  @Override
  public Optional<Message> enqueue(final String messageType, final String payload) {
    LOGGER.trace("enqueue({},{})", messageType, payload);
    return metrics.time("QueueImpl.enqueue", Tags.of("messageType", messageType), () -> {
      try {
        return messageManager.saveMessage(messageType, payload);
      } catch (RuntimeException e) {
        if (queueConfiguration.exceptionOnEnqueueFail()) {
          throw e;
        } else {
          return Optional.empty();
        }
      }
    });
  }

  @Override
  public Optional<State> getState(final Message message) {
    LOGGER.trace("getState({})", message);
    return messageManager.getState(message);
  }

  @Override
  public Map<State, Long> getMessageStateCounts() {
    return messageManager.counts();
  }

  @Override
  public void clearAll() {
    LOGGER.trace("clearAll()");
    messageManager.clearAll();
  }

  @Override
  public void clear(final Message message) {
    LOGGER.trace("clear({})", message);
    messageManager.clear(message);
  }
}
