package com.codeheadsystems.queue.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codeheadsystems.metrics.test.BaseMetricTest;
import com.codeheadsystems.queue.Message;
import com.codeheadsystems.queue.QueueConfiguration;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.factory.QueueConfigurationFactory;
import com.codeheadsystems.queue.manager.MessageManager;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueueProcessorTest extends BaseMetricTest {

  @Mock private MessageManager messageManager;
  @Mock private QueueConfiguration queueConfiguration;
  @Mock private MessageConsumerExecutor messageConsumerExecutor;
  @Mock private ScheduledExecutorService scheduledExecutorService;
  @Mock private Message message;

  @Mock private ScheduledFuture scheduler;

  private QueueProcessor processor;

  @BeforeEach
  void setup() {
    processor = new QueueProcessor(messageManager, new QueueConfigurationFactory(Optional.of(queueConfiguration)), messageConsumerExecutor, scheduledExecutorService, metricsFactory);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testStart() {
    when(queueConfiguration.queueProcessorInitialDelay()).thenReturn(1);
    when(queueConfiguration.queueProcessorInterval()).thenReturn(0);
    when(scheduledExecutorService.scheduleAtFixedRate(any(), eq(1L), eq(0L), eq(TimeUnit.SECONDS)))
        .thenReturn(scheduler);

    processor.start();
    processor.start();

    verify(messageManager, times(1)).setAllToPending();
  }

  @Test
  void testProcessingPendingQueue() {
    when(messageConsumerExecutor.availableThreadCount()).thenReturn(1);
    when(messageManager.getPendingMessages(1)).thenReturn(List.of(message));

    processor.processPendingQueue();

    verify(messageManager, times(1)).setActivating(message);
    verify(messageConsumerExecutor, times(1)).enqueue(message);
  }

  @Test
  void testProcessingPendingQueue_noActiveThreads() {
    when(messageConsumerExecutor.availableThreadCount()).thenReturn(0);

    processor.processPendingQueue();

    verifyNoInteractions(messageManager);
    verifyNoMoreInteractions(messageConsumerExecutor);
  }

}