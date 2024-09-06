package com.codeheadsystems.queue.dao;

import com.codeheadsystems.queue.State;
import org.immutables.value.Value;

/**
 * The interface State count.
 */
@Value.Immutable
public interface StateCount {

  /**
   * State state.
   *
   * @return the state
   */
  State state();

  /**
   * Count long.
   *
   * @return the long
   */
  long count();

}
