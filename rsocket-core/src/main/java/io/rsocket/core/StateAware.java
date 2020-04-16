package io.rsocket.core;

import javax.annotation.Nullable;

interface StateAware {

  @Nullable
  Throwable error();

  @Nullable
  Throwable checkAvailable();
}
