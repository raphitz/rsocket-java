package io.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A client-side interface to simplify interactions with the {@link
 * io.rsocket.core.RSocketConnector}
 */
public interface RSocketClient extends Closeable, Availability {

  /**
   * Fire and Forget interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  default Mono<Void> fireAndForget(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(this::fireAndForget);
  }

  /**
   * Fire and Forget interaction model of {@link RSocketClient}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  default Mono<Void> fireAndForget(Payload payload) {
    payload.release();
    return Mono.error(new UnsupportedOperationException("Fire-and-Forget not implemented."));
  }

  /**
   * Request-Response interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} containing at most a single {@code Payload} representing the
   *     response.
   */
  default Mono<Payload> requestResponse(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(this::requestResponse);
  }

  /**
   * Request-Response interaction model of {@link RSocketClient}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} containing at most a single {@code Payload} representing the
   *     response.
   */
  default Mono<Payload> requestResponse(Payload payload) {
    payload.release();
    return Mono.error(new UnsupportedOperationException("Request-Response not implemented."));
  }

  /**
   * Request-Stream interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} containing the stream of {@code Payload}s representing the response.
   */
  default Flux<Payload> requestStream(Mono<Payload> payloadMono) {
    return payloadMono.flatMapMany(this::requestStream);
  }

  /**
   * Request-Stream interaction model of {@link RSocketClient}.
   *
   * @param payload Request payload.
   * @return {@code Publisher} containing the stream of {@code Payload}s representing the response.
   */
  default Flux<Payload> requestStream(Payload payload) {
    payload.release();
    return Flux.error(new UnsupportedOperationException("Request-Stream not implemented."));
  }

  /**
   * Request-Channel interaction model of {@link RSocketClient}.
   *
   * @param payloads Stream of request payloads.
   * @return Stream of response payloads.
   */
  default Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
  }

  /**
   * Metadata-Push interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  default Mono<Void> metadataPush(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(this::metadataPush);
  }

  /**
   * Metadata-Push interaction model of {@link RSocketClient}.
   *
   * @param payload Request payloads.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  default Mono<Void> metadataPush(Payload payload) {
    payload.release();
    return Mono.error(new UnsupportedOperationException("Metadata-Push not implemented."));
  }

  /**
   * Provides access to the source {@link RSocket} used by this {@link RSocketClient}
   *
   * @return returns a {@link Mono} which returns the source {@link RSocket}
   */
  Mono<RSocket> source();

  @Override
  default double availability() {
    return isDisposed() ? 0.0 : 1.0;
  }

  @Override
  default void dispose() {}

  @Override
  default boolean isDisposed() {
    return false;
  }

  @Override
  default Mono<Void> onClose() {
    return Mono.never();
  }
}
