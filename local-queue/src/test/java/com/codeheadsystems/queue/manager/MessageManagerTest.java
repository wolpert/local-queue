package com.codeheadsystems.queue.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.codeheadsystems.metrics.test.BaseMetricTest;
import com.codeheadsystems.queue.State;
import com.codeheadsystems.queue.dao.ImmutableStateCount;
import com.codeheadsystems.queue.dao.MessageDao;
import com.codeheadsystems.queue.factory.MessageFactory;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageManagerTest extends BaseMetricTest {

  @Mock private MessageDao messageDao;
  @Mock private MessageFactory messageFactory;

  @InjectMocks private MessageManager messageManager;

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

}