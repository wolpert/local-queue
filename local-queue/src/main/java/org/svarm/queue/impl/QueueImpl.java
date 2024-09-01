package org.svarm.queue.impl;

import com.codeheadsystems.metrics.Metrics;
import com.codeheadsystems.metrics.Tags;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.svarm.queue.Message;
import org.svarm.queue.Queue;
import org.svarm.queue.QueueConfiguration;
import org.svarm.queue.State;
import org.svarm.queue.dao.MessageDao;
import org.svarm.queue.factory.MessageFactory;
import org.svarm.queue.factory.QueueConfigurationFactory;

/**
 * The type Queue.
 */
@Singleton
public class QueueImpl implements Queue {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueImpl.class);

  private final MessageDao messageDao;
  private final MessageFactory messageFactory;
  private final QueueConfiguration queueConfiguration;
  private final Metrics metrics;

  /**
   * Instantiates a new Queue.
   *
   * @param messageDao                the message dao
   * @param messageFactory            the message factory
   * @param queueConfigurationFactory the queue configuration factory
   * @param metrics                   the metrics
   */
  @Inject
  public QueueImpl(final MessageDao messageDao,
                   final MessageFactory messageFactory,
                   final QueueConfigurationFactory queueConfigurationFactory,
                   final Metrics metrics) {
    this.messageDao = messageDao;
    this.messageFactory = messageFactory;
    this.queueConfiguration = queueConfigurationFactory.queueConfiguration();
    this.metrics = metrics;
    LOGGER.info("QueueImpl({}, {},{})", queueConfiguration, messageDao, messageFactory);
  }

  @Override
  public Optional<Message> enqueue(final String messageType, final String payload) {
    LOGGER.trace("enqueue({},{})", messageType, payload);
    return metrics.time("QueueImpl.enqueue", Tags.of("messageType", messageType), () -> {
      final Message message = messageFactory.createMessage(messageType, payload);
      try {
        messageDao.store(message, State.PENDING);
        return Optional.of(message);
      } catch (final UnableToExecuteStatementException e) {
        if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
          LOGGER.warn("Message already exists: {}", message);
          final Message existingMessage = messageDao.readByHash(message.hash()) // lookup since the UUID could be different
              .orElseThrow(() -> new IllegalStateException("Message should exist: " + message));
          return Optional.of(existingMessage);
        } else {
          if (queueConfiguration.exceptionOnEnqueueFail()) {
            LOGGER.error("Unable to store message: {}", message, e);
            throw e;
          } else {
            LOGGER.warn("Unable to store message: {}", message, e);
            return Optional.empty();
          }
        }
      }
    });
  }

  @Override
  public Optional<State> getState(final Message message) {
    LOGGER.trace("getState({})", message);
    return messageDao.stateOf(message);
  }

  @Override
  public void clearAll() {
    LOGGER.trace("clearAll()");
    messageDao.deleteAll();
  }

  @Override
  public void clear(final Message message) {
    LOGGER.trace("clear({})", message);
    messageDao.delete(message);
  }
}
