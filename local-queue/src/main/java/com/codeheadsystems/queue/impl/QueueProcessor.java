package com.codeheadsystems.queue.impl;

import com.codeheadsystems.metrics.Metrics;
import com.codeheadsystems.queue.QueueConfiguration;
import com.codeheadsystems.queue.factory.QueueConfigurationFactory;
import com.codeheadsystems.queue.manager.MessageManager;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Queue Processor. Reads the queues and creates workers for the messages.
 */
@Singleton
public class QueueProcessor implements Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueueProcessor.class);

  private final MessageManager messageManager;
  private final QueueConfiguration queueConfiguration;
  private final MessageConsumerExecutor messageConsumerExecutor;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Metrics metrics;
  private ScheduledFuture<?> scheduler;

  /**
   * Instantiates a new Queue processor.
   *
   * @param messageManager            the messageManager
   * @param queueConfigurationFactory the queue configuration factory
   * @param messageConsumerExecutor   the message consumer executor
   * @param metrics                   the metrics
   */
  @Inject
  public QueueProcessor(final MessageManager messageManager,
                        final QueueConfigurationFactory queueConfigurationFactory,
                        final MessageConsumerExecutor messageConsumerExecutor,
                        final Metrics metrics) {
    this(messageManager,
        queueConfigurationFactory,
        messageConsumerExecutor,
        Executors.newScheduledThreadPool(1),
        metrics);
  }

  @VisibleForTesting
  QueueProcessor(final MessageManager messageManager,
                 final QueueConfigurationFactory queueConfigurationFactory,
                 final MessageConsumerExecutor messageConsumerExecutor,
                 final ScheduledExecutorService scheduledExecutorService,
                 final Metrics metrics) {
    this.messageManager = messageManager;
    this.queueConfiguration = queueConfigurationFactory.queueConfiguration();
    this.messageConsumerExecutor = messageConsumerExecutor;
    this.scheduledExecutorService = scheduledExecutorService;
    this.metrics = metrics;
    LOGGER.info("QueueProcessor({},{},{})", messageManager, queueConfiguration, messageConsumerExecutor);
  }

  @Override
  public void start() {
    LOGGER.info("start()");
    synchronized (scheduledExecutorService) {
      if (scheduler == null) {
        LOGGER.info("Resetting existing messages to pending state");
        messageManager.setAllToPending();
        LOGGER.info("Starting the scheduler");
        scheduler = scheduledExecutorService.scheduleAtFixedRate(this::processPendingQueue,
            queueConfiguration.queueProcessorInitialDelay(),
            queueConfiguration.queueProcessorInterval(),
            TimeUnit.SECONDS);
      }
    }
    LOGGER.info("Queue accepting messages");
  }

  /**
   * Process pending queue.
   */
  public void processPendingQueue() {
    LOGGER.trace("processPendingQueue()");
    final int messageCount = messageConsumerExecutor.availableThreadCount();
    metrics.increment("QueueProcessor.processPendingQueue.availableThreads", messageCount);
    if (messageCount < 1) {
      LOGGER.trace("No threads available to process messages: {}", messageCount);
      return;
    }
    metrics.time("QueueProcessor.processPendingQueue", () -> {
      messageManager.getPendingMessages(messageCount).forEach(message -> {
        LOGGER.trace("Processing message {}", message);
        messageManager.setActivating(message);
        messageConsumerExecutor.enqueue(message);
      });
      return null;
    });
  }

  @Override
  public void stop() throws Exception {
    LOGGER.info("stop()");
    synchronized (scheduledExecutorService) {
      if (scheduler != null) {
        LOGGER.info("Shutting down the scheduler");
        scheduler.cancel(true);
        scheduler = null;
        LOGGER.info("Shutting down the scheduler service");
        scheduledExecutorService.shutdown();
        if (!scheduledExecutorService.awaitTermination(15, TimeUnit.SECONDS)) {
          LOGGER.info("Shutting down nicely failed. No longer being nice.");
          scheduledExecutorService.shutdownNow();
        }
      }
    }
    LOGGER.info("Queue no longer accepting messages");
  }
}
