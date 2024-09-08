package com.codeheadsystems.queue.module;

import com.codeheadsystems.queue.MessageConsumer;
import com.codeheadsystems.queue.Queue;
import com.codeheadsystems.queue.QueueConfiguration;
import com.codeheadsystems.queue.dao.MessageDao;
import com.codeheadsystems.queue.impl.MessageConsumerExecutor;
import com.codeheadsystems.queue.impl.QueueImpl;
import com.codeheadsystems.queue.impl.QueueProcessor;
import dagger.Binds;
import dagger.BindsOptionalOf;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import dagger.multibindings.Multibinds;
import io.dropwizard.lifecycle.Managed;
import java.util.Map;
import javax.inject.Singleton;
import org.jdbi.v3.core.Jdbi;

/**
 * The type Queue module.
 */
@Module(includes = QueueModule.Binder.class)
public class QueueModule {

  /**
   * Instantiates a new Queue module.
   */
  public QueueModule() {
  }

  /**
   * Queue queue.
   *
   * @param queue the queue
   * @return the queue
   */
  @Singleton
  @Provides
  public Queue queue(final QueueImpl queue) {
    return queue;
  }

  /**
   * Message dao message dao.
   *
   * @param jdbi the jdbi, which we require already has the SQLObjects and immutable plugin installed.
   * @return the message dao
   */
  @Singleton
  @Provides
  public MessageDao messageDao(final Jdbi jdbi) {
    return MessageDao.instance(jdbi);
  }

  /**
   * The interface Binder.
   */
  @Module
  interface Binder {


    /**
     * Queue configuration queue configuration. If you don't define one, we use the default.
     *
     * @return the queue configuration
     */
    @BindsOptionalOf
    QueueConfiguration queueConfiguration();

    /**
     * Managed instance of the queue processor for the runtimes.
     *
     * @param queueProcessor the queue processor
     * @return the managed
     */
    @IntoSet
    @Binds
    Managed managedQueueProcessor(final QueueProcessor queueProcessor);

    /**
     * Managed message consumer executor managed.
     *
     * @param messageConsumerExecutor the message consumer executor
     * @return the managed
     */
    @IntoSet
    @Binds
    Managed managedMessageConsumerExecutor(final MessageConsumerExecutor messageConsumerExecutor);

    /**
     * Message consumers map.
     *
     * @return the map
     */
    @Multibinds
    Map<String, MessageConsumer> messageConsumers();

  }

}
