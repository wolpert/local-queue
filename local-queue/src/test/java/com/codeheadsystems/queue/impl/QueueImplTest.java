package com.codeheadsystems.queue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codeheadsystems.metrics.test.BaseMetricTest;
import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.QueueConfiguration;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.factory.QueueConfigurationFactory;
import com.codeheadsystems.queue.manager.MessageManager;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Optional;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueueImplTest extends BaseMetricTest {

  private static final String TYPE = "messageType";
  private static final String PAYLOAD = "payload";
  private static final String UUID = "uuid";
  private static final String HASH = "hash";
  @Mock private MessageManager messageManager;
  @Mock private QueueConfiguration queueConfiguration;

  @Mock private Message message;
  @Mock private Message lookupMessage;
  @Mock private UnableToExecuteStatementException unableToExecuteStatementException;
  @Mock private SQLIntegrityConstraintViolationException sqlIntegrityConstraintViolationException;

  private QueueImpl queue;

  @BeforeEach
  public void setup() {
    queue = new QueueImpl(messageManager, new QueueConfigurationFactory(Optional.of(queueConfiguration)), metricsFactory);
  }

  @Test
  void enqueue() {
    when(messageManager.saveMessage(TYPE, PAYLOAD)).thenReturn(Optional.of(message));

    assertThat(queue.enqueue(TYPE, PAYLOAD))
        .isNotEmpty()
        .contains(message);
  }

  @Test
  void enqueue_failureToSaveMessage_configDisablesException() {
    when(queueConfiguration.exceptionOnEnqueueFail()).thenReturn(false);
    when(messageManager.saveMessage(TYPE, PAYLOAD)).thenThrow(unableToExecuteStatementException); // not dup

    assertThat(queue.enqueue(TYPE, PAYLOAD))
        .isEmpty();
  }

  @Test
  void enqueue_failureToSaveMessage_configEnablesException() {
    when(queueConfiguration.exceptionOnEnqueueFail()).thenReturn(true);
    when(messageManager.saveMessage(TYPE, PAYLOAD)).thenThrow(unableToExecuteStatementException); // not dup

    assertThatExceptionOfType(UnableToExecuteStatementException.class)
        .isThrownBy(() -> queue.enqueue(TYPE, PAYLOAD));
  }

  @Test
  void getState() {
    when(messageManager.getState(message)).thenReturn(Optional.of(State.ACTIVATING));
    assertThat(queue.getState(message)).contains(State.ACTIVATING);
  }

  @Test
  void delete() {
    queue.clear(message);
    verify(messageManager).clear(message);
  }

  @Test
  void deleteAll() {
    queue.clearAll();
    verify(messageManager).clearAll();
  }
}