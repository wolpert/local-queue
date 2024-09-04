package com.codeheadsystems.queue.dao;

import static java.time.Instant.EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.factory.MessageFactory;
import com.codeheadsystems.queue.module.QueueModule;
import com.codeheadsystems.queue.util.LiquibaseHelper;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageDaoTest {

  @Mock private Clock clock;

  private MessageFactory messageFactory;
  private Jdbi jdbi;
  private DataSource dataSource;
  private MessageDao messageDao;

  @Test
  void testRoundTrip() {
    when(clock.instant()).thenReturn(EPOCH);
    final Message message = messageFactory.createMessage("type", "payload");
    messageDao.store(message, State.ACTIVATING);
    final Optional<Message> result = messageDao.readByHash(message.hash());
    assertThat(result)
        .isNotEmpty()
        .contains(message);
    messageDao.delete(message);
    assertThat(messageDao.readByHash(message.hash()))
        .isEmpty();
  }

  @Test
  void testHashLookup() {
    when(clock.instant()).thenReturn(EPOCH);
    final Message message = messageFactory.createMessage("type", "payload");
    messageDao.store(message, State.ACTIVATING);
    assertThat(messageDao.readByHash(message.hash()))
        .isNotEmpty()
        .contains(message);
  }

  @Test
  void testUpdateState() {
    when(clock.instant()).thenReturn(EPOCH);
    final Message message = messageFactory.createMessage("type", "payload");
    messageDao.store(message, State.ACTIVATING);
    assertThat(messageDao.forState(State.ACTIVATING)).containsExactly(message);
    assertThat(messageDao.stateOf(message)).contains(State.ACTIVATING);
    messageDao.updateState(message, State.PENDING);
    assertThat(messageDao.stateOf(message)).contains(State.PENDING);
    assertThat(messageDao.forState(State.PENDING)).containsExactly(message);
    assertThat(messageDao.forState(State.ACTIVATING)).isEmpty();
  }

  @Test
  void testDupPayload() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(100));
    final Message message1 = messageFactory.createMessage("type", "payload");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(101));
    final Message message2 = messageFactory.createMessage("type", "payload");
    messageDao.store(message1, State.ACTIVATING);
    assertThatExceptionOfType(UnableToExecuteStatementException.class)
        .isThrownBy(() -> messageDao.store(message2, State.ACTIVATING))
        .withCauseInstanceOf(SQLIntegrityConstraintViolationException.class);
  }

  @Test
  void testDeleteAll() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(100));
    final Message message1 = messageFactory.createMessage("type", "payload1");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(101));
    final Message message2 = messageFactory.createMessage("type", "payload2");
    messageDao.store(message1, State.ACTIVATING);
    messageDao.store(message2, State.ACTIVATING);
    assertThat(messageDao.forState(State.ACTIVATING)).hasSize(2).containsExactly(message1, message2);

    messageDao.deleteAll();
    assertThat(messageDao.forState(State.ACTIVATING)).isEmpty();
  }

  @Test
  void testListByState() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(100));
    final Message message1 = messageFactory.createMessage("type", "payload:1");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(110));
    final Message message2 = messageFactory.createMessage("type", "payload:2");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(120));
    final Message message3 = messageFactory.createMessage("type", "payload:3");
    messageDao.store(message1, State.ACTIVATING);
    messageDao.store(message2, State.PENDING);
    messageDao.store(message3, State.ACTIVATING);
    final List<Message> list = messageDao.forState(State.ACTIVATING);
    assertThat(list)
        .hasSize(2)
        .containsExactly(message1, message3);
  }

  @Test
  void testListByStateWithLimit() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(100));
    final Message message1 = messageFactory.createMessage("type", "payload:1");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(110));
    final Message message2 = messageFactory.createMessage("type", "payload:2");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(120));
    final Message message3 = messageFactory.createMessage("type", "payload:3");
    messageDao.store(message1, State.ACTIVATING);
    messageDao.store(message2, State.PENDING);
    messageDao.store(message3, State.ACTIVATING);
    final List<Message> list = messageDao.forState(State.ACTIVATING, 1);
    assertThat(list)
        .hasSize(1)
        .containsExactly(message1);
    final List<Message> list2 = messageDao.forState(State.ACTIVATING, 3);
    assertThat(list2)
        .hasSize(2)
        .containsExactly(message1, message3);
  }

  @Test
  void testUpdateAllToPending() {
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(100));
    final Message message1 = messageFactory.createMessage("type", "payload:1");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(110));
    final Message message2 = messageFactory.createMessage("type", "payload:2");
    when(clock.instant()).thenReturn(Instant.ofEpochMilli(120));
    final Message message3 = messageFactory.createMessage("type", "payload:3");
    messageDao.store(message1, State.ACTIVATING);
    messageDao.store(message2, State.PENDING);
    messageDao.store(message3, State.ACTIVATING);
    final List<Message> list = messageDao.forState(State.PENDING, 1);
    assertThat(list)
        .hasSize(1)
        .containsExactly(message2);
    messageDao.updateAllToState(State.PENDING);
    final List<Message> list2 = messageDao.forState(State.PENDING, 3);
    assertThat(list2)
        .hasSize(3);
  }

  @BeforeEach
  void setup() throws SQLException {
    messageFactory = new MessageFactory(clock);
    dataSource = dataSource();
    new LiquibaseHelper().runLiquibase(dataSource, "liquibase/queue.xml");
    jdbi = Jdbi.create(dataSource);
    jdbi.installPlugin(new SqlObjectPlugin());
    messageDao = new QueueModule().messageDao(jdbi);
  }

  @AfterEach
  void shutdownSQLEngine() {
    Jdbi.create(dataSource).withHandle(handle -> handle.execute("shutdown;"));
  }

  private DataSource dataSource() {
    final String url = "jdbc:hsqldb:mem:" + getClass().getSimpleName() + ":" + UUID.randomUUID();
    final ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setJdbcUrl(url);
    cpds.setUser("SA");
    cpds.setPassword("");
    cpds.setMinPoolSize(0);
    cpds.setAcquireIncrement(10);
    cpds.setMaxPoolSize(40);
    cpds.setMaxIdleTime(300);
    return cpds;
  }


}