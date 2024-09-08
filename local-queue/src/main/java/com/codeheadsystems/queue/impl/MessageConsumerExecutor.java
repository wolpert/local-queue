package com.codeheadsystems.queue.impl;

import com.codeheadsystems.metrics.Metrics;
import com.codeheadsystems.metrics.Tags;
import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.MessageConsumer;
import com.codeheadsystems.queue.QueueConfiguration;
import com.codeheadsystems.queue.factory.QueueConfigurationFactory;
import com.codeheadsystems.queue.manager.MessageManager;
import io.dropwizard.lifecycle.Managed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Message consumer executor.
 */
@Singleton
public class MessageConsumerExecutor implements Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumerExecutor.class);

  private final ThreadPoolExecutor executorService;
  private final MessageManager messageManager;
  private final QueueRegister queueRegister;
  private final Metrics metrics;

  /**
   * Instantiates a new Message consumer executor.
   *
   * @param queueConfigurationFactory the configuration.
   * @param messageManager            the message manager
   * @param queueRegister             the queue register
   * @param metrics                   the metrics
   */
  @Inject
  public MessageConsumerExecutor(final QueueConfigurationFactory queueConfigurationFactory,
                                 final MessageManager messageManager,
                                 final QueueRegister queueRegister,
                                 final Metrics metrics) {
    final QueueConfiguration configuration = queueConfigurationFactory.queueConfiguration();
    this.executorService = new ThreadPoolExecutor(
        configuration.queueExecutorMinThreads(),
        configuration.queueExecutorMaxThreads(),
        configuration.queueExecutorIdleSeconds(), TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    this.messageManager = messageManager;
    this.queueRegister = queueRegister;
    this.metrics = metrics;
    LOGGER.info("MessageConsumerExecutor({},{},{})", messageManager, executorService, queueRegister);
  }

  /**
   * Number of free threads in the executor service.
   *
   * @return the number of free threads.
   */
  public int availableThreadCount() {
    return executorService.getMaximumPoolSize() - executorService.getActiveCount();
  }

  /**
   * Enqueue.
   *
   * @param message the message
   */
  public void enqueue(final Message message) {
    LOGGER.trace("enqueue({})", message);
    final String messageType = message.messageType();
    metrics.time("MessageConsumerExecutor.enqueue", Tags.of("messageType", messageType), () -> {
      queueRegister.getConsumer(messageType)
          .ifPresentOrElse(
              messageConsumer -> executorService.execute(() -> execute(message, messageConsumer)),
              () -> {
                LOGGER.error("No message for type {}", message.messageType());
                messageManager.clear(message);
              });
      return null;
    });
  }

  private void execute(final Message message, final MessageConsumer consumer) {
    LOGGER.trace("execute({},{})", message, consumer);
    try {
      metrics.time("MessageConsumerExecutor.execute", Tags.of("messageType", message.messageType()), () -> {
        messageManager.setProcessing(message);
        consumer.accept(message);
        return null;
      });
    } catch (final Throwable t) {
      // There is no dead letter queue... an no poison pill impact. We delete either way.
      LOGGER.error("Error processing message: {}", message, t); // do not die
    } finally {
      messageManager.clear(message);
    }
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("Executor service enabled to start executing messages");
  }

  @Override
  public void stop() throws Exception {
    LOGGER.info("stop()");
    LOGGER.info("Shutting down the executor service");
    executorService.shutdown();
    if (!executorService.awaitTermination(15, TimeUnit.SECONDS)) {
      LOGGER.info("Shutting down nicely failed. No longer being nice.");
      executorService.shutdownNow().forEach(runnable -> LOGGER.warn("Unable to shutdown {}", runnable));
    }
    LOGGER.info("Executor service no longer executing messages");
  }
}
