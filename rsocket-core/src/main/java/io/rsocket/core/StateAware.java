package io.rsocket.core;

interface StateAware {

  Throwable error();

  Throwable checkAvailable();
}
