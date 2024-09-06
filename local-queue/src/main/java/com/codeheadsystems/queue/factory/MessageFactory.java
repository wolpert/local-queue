package com.codeheadsystems.queue.factory;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.codeheadsystems.queue.ImmutableMessage;
import com.codeheadsystems.queue.Message;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.time.Clock;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Message factory.
 */
@Singleton
public class MessageFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageFactory.class);
  private final Clock clock;
  private final LoadingCache<String, HashFunction> hashFunctionCache;

  /**
   * Instantiates a new Message factory.
   *
   * @param clock the clock
   */
  @Inject
  public MessageFactory(final Clock clock) {
    this.clock = clock;
    this.hashFunctionCache = CacheBuilder.newBuilder()
        .maximumSize(10) // TODO: Set this ina configuration
        .build(CacheLoader.from(this::generateHashFunction));
    LOGGER.info("MessageFactory({})", clock);
  }

  private HashFunction generateHashFunction(final String messageType) {
    return Hashing.hmacSha512(messageType.getBytes(UTF_8));
  }

  /**
   * Create message message.
   *
   * @param messageType the message type
   * @param payload     the payload
   * @return the message
   */
  public Message createMessage(final String messageType,
                               final String payload) {
    LOGGER.trace("createMessage({},{})", messageType, payload);
    final HashFunction hashFunction = hashFunctionCache.getUnchecked(messageType);
    return ImmutableMessage.builder()
        .timestamp(clock.instant().toEpochMilli())
        .messageType(messageType)
        .payload(payload)
        .hash(hashFunction.hashString(payload, Charsets.UTF_8).asLong())
        .build();
  }

}
