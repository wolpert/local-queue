package com.codeheadsystems.queue.manager;

import com.codeheadsystems.metrics.Metrics;
import com.codeheadsystems.metrics.Tags;
import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.dao.MessageDao;
import com.codeheadsystems.queue.dao.StateCount;
import com.codeheadsystems.queue.factory.MessageFactory;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Message manager. This links the DAO with the model. Right now they
 * are tightly coupled, but by only having the DAO used here, we enforce
 * that should it need to change we can.
 */
@Singleton
public class MessageManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageManager.class);

  private final MessageDao dao;
  private final MessageFactory messageFactory;
  private final Metrics metrics;

  /**
   * Instantiates a new Message manager.
   *
   * @param dao            the dao
   * @param messageFactory the message factory
   * @param metrics        the metrics
   */
  @Inject
  public MessageManager(final MessageDao dao,
                        final MessageFactory messageFactory,
                        final Metrics metrics) {
    this.dao = dao;
    this.messageFactory = messageFactory;
    this.metrics = metrics;
  }

  /**
   * Save message optional.
   *
   * @param messageType the message type
   * @param payload     the payload
   * @return the optional
   */
  public Optional<Message> saveMessage(final String messageType, final String payload) {
    LOGGER.trace("saveMessage({},{})", messageType, payload);
    return metrics.time("MessageManager.saveMessage", Tags.of("messageType", messageType), () -> {
      final Message message = messageFactory.createMessage(messageType, payload);
      try {
        dao.store(message, State.PENDING);
        return Optional.of(message);
      } catch (final UnableToExecuteStatementException e) {
        if (e.getCause() instanceof SQLIntegrityConstraintViolationException) {
          LOGGER.warn("Message already exists: {}", message);
          final Message existingMessage = dao.readByHash(message.hash()) // lookup since the UUID could be different
              .orElseThrow(() -> new IllegalStateException("Message should exist: " + message));
          return Optional.of(existingMessage);
        } else {
          LOGGER.error("Unable to store message: {}", message, e);
          throw e;
        }
      }
    });
  }

  /**
   * Sets processing.
   *
   * @param message the message
   */
  public void setProcessing(final Message message) {
    LOGGER.trace("setProcessing({})", message);
    dao.updateState(message, State.PROCESSING);
  }

  /**
   * Sets activating.
   *
   * @param message the message
   */
  public void setActivating(final Message message) {
    LOGGER.trace("setActivation({})", message);
    dao.updateState(message, State.ACTIVATING);
  }

  /**
   * Sets all to pending.
   */
  public void setAllToPending() {
    LOGGER.trace("setAllToPending()");
    dao.updateAllToState(State.PENDING);
  }

  /**
   * Gets pending messages, up to the limit. Oldest first.
   *
   * @param limit count of messages to get.
   * @return the list.
   */
  public List<Message> getPendingMessages(final int limit) {
    LOGGER.trace("getPendingMessages({})", limit);
    return dao.forState(State.PENDING, limit);
  }

  /**
   * Returns counts of all states.
   *
   * @return the map
   */
  public Map<State, Long> counts() {
    LOGGER.trace("counts()");
    return dao.counts().stream().collect(Collectors.toMap(StateCount::state, StateCount::count));
  }

  /**
   * Gets state.
   *
   * @param message the message
   * @return the state
   */
  public Optional<State> getState(final Message message) {
    LOGGER.trace("getState({})", message);
    return dao.stateOf(message);
  }

  /**
   * Clear all.
   */
  public void clearAll() {
    LOGGER.trace("clearAll()");
    dao.deleteAll();
  }

  /**
   * Clear.
   *
   * @param message the message
   */
  public void clear(final Message message) {
    LOGGER.trace("clear({})", message);
    dao.delete(message);
  }

}
