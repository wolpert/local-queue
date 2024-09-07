package com.codeheadsystems.queue.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codeheadsystems.metrics.test.BaseMetricTest;
import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.dao.ImmutableStateCount;
import com.codeheadsystems.queue.dao.MessageDao;
import com.codeheadsystems.queue.factory.MessageFactory;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Optional;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageManagerTest extends BaseMetricTest {

  private static final String PAYLOAD = "payload";
  private static final String TYPE = "type";
  private static final long HASH = 1234L;
  @Mock private MessageDao messageDao;
  @Mock private MessageFactory messageFactory;
  @Mock private Message message;
  @Mock private StatementContext statementContext;
  @Mock private UnableToExecuteStatementException unableToExecuteStatementException;

  private MessageManager messageManager;

  @BeforeEach
  void setup() {
    messageManager = new MessageManager(messageDao, messageFactory, metricsFactory);
  }

  @Test
  void counts_empty() {
    when(messageDao.counts()).thenReturn(List.of());
    assertThat(messageManager.counts()).isEmpty();
  }

  @Test
  void counts() {
    when(messageDao.counts()).thenReturn(List.of(
        ImmutableStateCount.builder().state(State.ACTIVATING).count(1).build(),
        ImmutableStateCount.builder().state(State.PENDING).count(5).build()
    ));
    assertThat(messageManager.counts())
        .hasSize(2)
        .containsEntry(State.ACTIVATING, 1L)
        .containsEntry(State.PENDING, 5L);
  }

  @Test
  void saveMessage() {
    when(messageFactory.createMessage(TYPE, PAYLOAD)).thenReturn(message);
    assertThat(messageManager.saveMessage(TYPE, PAYLOAD))
        .isPresent()
        .contains(message);
    verify(messageDao).store(message, State.PENDING);
  }

  @Test
  void saveMessage_duplicate() {
    when(messageFactory.createMessage(TYPE, PAYLOAD)).thenReturn(message);
    when(message.hash()).thenReturn(HASH);
    when(messageDao.readByHash(HASH)).thenReturn(Optional.of(message));
    when(messageManager.saveMessage(TYPE, PAYLOAD))
        .thenThrow(unableToExecuteStatementException);
    when(unableToExecuteStatementException.getCause())
        .thenReturn(new SQLIntegrityConstraintViolationException());
    assertThat(messageManager.saveMessage(TYPE, PAYLOAD))
        .isPresent()
        .contains(message);
  }

  @Test
  void saveMessage_saveFailure() {
    when(messageFactory.createMessage(TYPE, PAYLOAD)).thenReturn(message);
    when(messageManager.saveMessage(TYPE, PAYLOAD))
        .thenThrow(unableToExecuteStatementException);
    when(unableToExecuteStatementException.getCause())
        .thenReturn(new RuntimeException());
    assertThatExceptionOfType(UnableToExecuteStatementException.class)
        .isThrownBy(() -> messageManager.saveMessage(TYPE, PAYLOAD));
  }

  @Test
  void setProcessing() {
    messageManager.setProcessing(message);
    verify(messageDao).updateState(message, State.PROCESSING);
  }

  @Test
  void setActivating() {
    messageManager.setActivating(message);
    verify(messageDao).updateState(message, State.ACTIVATING);
  }

  @Test
  void setAllToPending() {
    messageManager.setAllToPending();
    verify(messageDao).updateAllToState(State.PENDING);
  }

  @Test
  void getPendingMessages() {
    when(messageDao.forState(State.PENDING, 1)).thenReturn(List.of(message));
    assertThat(messageManager.getPendingMessages(1))
        .containsExactly(message);
  }

  @Test
  void getPendingMessages_empty() {
    when(messageDao.forState(State.PENDING, 1)).thenReturn(List.of());
    assertThat(messageManager.getPendingMessages(1)).isEmpty();
  }

  @Test
  void getState() {
    when(messageDao.stateOf(message)).thenReturn(Optional.of(State.PENDING));
    assertThat(messageManager.getState(message)).contains(State.PENDING);
  }

  @Test
  void getState_empty() {
    when(messageDao.stateOf(message)).thenReturn(Optional.empty());
    assertThat(messageManager.getState(message)).isEmpty();
  }

  @Test
  void delete() {
    messageManager.clear(message);
    verify(messageDao).delete(message);
  }

  @Test
  void deleteAll() {
    messageManager.clearAll();
    verify(messageDao).deleteAll();
  }

}