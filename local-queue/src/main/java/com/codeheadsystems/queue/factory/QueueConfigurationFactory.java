package com.codeheadsystems.queue.factory;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.codeheadsystems.queue.ImmutableQueueConfiguration;
import com.codeheadsystems.queue.QueueConfiguration;

/**
 * Not really a factory. Just a way to get the configuration from the optional.
 */
@Singleton
public class QueueConfigurationFactory {

  private final QueueConfiguration queueConfiguration;


  /**
   * Instantiates a new Queue configuration factory.
   *
   * @param optionalQueueConfiguration the optional queue configuration
   */
  @Inject
  public QueueConfigurationFactory(final Optional<QueueConfiguration> optionalQueueConfiguration) {
    this.queueConfiguration = optionalQueueConfiguration
        .orElseGet(() -> ImmutableQueueConfiguration.builder().build());
  }

  /**
   * Queue configuration queue configuration.
   *
   * @return the queue configuration
   */
  public QueueConfiguration queueConfiguration() {
    return queueConfiguration;
  }

}
