package com.codeheadsystems.queue.dao;

import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.State;
import java.util.List;
import java.util.Optional;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

/**
 * The interface Message dao.
 */
public interface MessageDao {

  /**
   * Instance message dao. In theory you can call this multiple times,
   * but it is recommended you only call it once.
   *
   * @param jdbi the jdbi
   * @return the message dao
   */
  static MessageDao instance(final Jdbi jdbi) {
    jdbi.getConfig(JdbiImmutables.class)
        .registerImmutable(StateCount.class)
        .registerImmutable(Message.class);
    return jdbi.onDemand(MessageDao.class);
  }

  /**
   * Store.
   *
   * @param message the message
   * @param state   the state
   */
  @SqlUpdate("insert into QUEUE (HASH, TIMESTAMP, MESSAGE_TYPE, PAYLOAD, STATE) "
      + "values (:hash, :timestamp, :messageType, :payload, :state)")
  void store(@BindPojo final Message message, @Bind("state") final State state);

  /**
   * Read by hash optional.
   *
   * @param hash the hash
   * @return the optional
   */
  @SqlQuery("select * from QUEUE where HASH = :hash")
  Optional<Message> readByHash(@Bind("hash") final long hash);

  /**
   * State of optional.
   *
   * @param message the message
   * @return the optional
   */
  @SqlQuery("select STATE from QUEUE where HASH = :hash")
  Optional<State> stateOf(@BindPojo final Message message);

  /**
   * Returns back the count of all messages in the queue.
   *
   * @return the list
   */
  @SqlQuery("select STATE, count(*) as COUNT from QUEUE group by STATE")
  List<StateCount> counts();

  /**
   * For state list.
   *
   * @param state the state
   * @return the list
   */
  @SqlQuery("select * from QUEUE where STATE = :state order by TIMESTAMP asc")
  List<Message> forState(@Bind("state") final State state);

  /**
   * For state list, but limit to the number requested.
   *
   * @param state the state
   * @param limit the max number of results you want.
   * @return the list
   */
  @SqlQuery("select * from QUEUE where STATE = :state order by TIMESTAMP asc limit :limit")
  List<Message> forState(@Bind("state") final State state, @Bind("limit") final int limit);

  /**
   * Update state.
   *
   * @param message the message
   * @param state   the state
   */
  @SqlUpdate("update QUEUE set STATE = :state where HASH = :hash")
  void updateState(@BindPojo final Message message, @Bind("state") final State state);

  /**
   * Update state.
   *
   * @param state the state
   */
  @SqlUpdate("update QUEUE set STATE = :state")
  void updateAllToState(@Bind("state") final State state);

  /**
   * Delete.
   *
   * @param message the message
   */
  @SqlUpdate("delete from QUEUE where HASH = :hash")
  void delete(@BindPojo final Message message);

  /**
   * Delete all.
   */
  @SqlUpdate("delete from QUEUE")
  void deleteAll();
}
