package io.rsocket.core;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketClient;
import io.rsocket.frame.FrameType;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

class DefaultRSocketClient extends ResolvingOperator<RSocket>
    implements CoreSubscriber<RSocket>, CorePublisher<RSocket>, RSocketClient {
  private static final Consumer<ReferenceCounted> DROPPED_ELEMENTS_CONSUMER =
      referenceCounted -> {
        if (referenceCounted.refCnt() > 0) {
          try {
            referenceCounted.release();
          } catch (IllegalReferenceCountException e) {
            // ignored
          }
        }
      };

  final Mono<RSocket> source;

  volatile Subscription s;

  static final AtomicReferenceFieldUpdater<DefaultRSocketClient, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(DefaultRSocketClient.class, Subscription.class, "s");

  DefaultRSocketClient(Mono<RSocket> source) {
    this.source = source;
  }

  @Override
  public Mono<Void> fireAndForget(Mono<Payload> payloadMono) {
    return payloadMono
        .flatMap(this::fireAndForget)
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return new MonoFlattingInner<>(this, payload, FrameType.REQUEST_FNF);
  }

  @Override
  public Mono<Payload> requestResponse(Mono<Payload> payloadMono) {
    return payloadMono
        .flatMap(this::requestResponse)
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return new MonoFlattingInner<>(this, payload, FrameType.REQUEST_RESPONSE);
  }

  @Override
  public Flux<Payload> requestStream(Mono<Payload> payloadMono) {
    return payloadMono
        .flatMapMany(this::requestStream)
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return new FluxFlattingInner<>(this, payload, FrameType.REQUEST_STREAM);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return new FluxFlattingInner<>(this, payloads, FrameType.REQUEST_CHANNEL);
  }

  @Override
  public Mono<Void> metadataPush(Mono<Payload> payloadMono) {
    return payloadMono
        .flatMap(this::metadataPush)
        .doOnDiscard(ReferenceCounted.class, DROPPED_ELEMENTS_CONSUMER);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return new MonoFlattingInner<>(this, payload, FrameType.METADATA_PUSH);
  }

  @Override
  public Mono<RSocket> source() {
    return Mono.fromDirect(this);
  }

  @Override
  @SuppressWarnings("uncheked")
  public void subscribe(CoreSubscriber<? super RSocket> actual) {
    final ResolvingOperator.MonoDeferredResolutionOperator<RSocket> inner =
        new ResolvingOperator.MonoDeferredResolutionOperator<>(actual, this);
    actual.onSubscribe(inner);

    this.observe(inner);
  }

  @Override
  public void subscribe(Subscriber<? super RSocket> s) {
    subscribe(Operators.toCoreSubscriber(s));
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (Operators.setOnce(S, this, s)) {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onComplete() {
    final Subscription s = this.s;
    final RSocket value = this.value;

    if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
      this.doFinally();
      return;
    }

    if (value == null) {
      this.terminate(new IllegalStateException("Source completed empty"));
    } else {
      this.complete(value);
    }
  }

  @Override
  public void onError(Throwable t) {
    final Subscription s = this.s;

    if (s == Operators.cancelledSubscription()
        || S.getAndSet(this, Operators.cancelledSubscription())
            == Operators.cancelledSubscription()) {
      this.doFinally();
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.doFinally();
    // terminate upstream which means retryBackoff has exhausted
    this.terminate(t);
  }

  @Override
  public void onNext(RSocket value) {
    if (this.s == Operators.cancelledSubscription()) {
      this.doOnValueExpired(value);
      return;
    }

    this.value = value;
    // volatile write and check on racing
    this.doFinally();
  }

  @Override
  protected void doSubscribe() {
    source.subscribe(this);
  }

  @Override
  protected void doOnValueResolved(RSocket value) {
    value.onClose().subscribe(null, t -> this.invalidate(), this::invalidate);
  }

  @Override
  protected void doOnValueExpired(RSocket value) {
    value.dispose();
  }

  @Override
  protected void doOnDispose() {
    Operators.terminate(S, this);
  }

  static final class MonoFlattingInner<T>
      extends ResolvingOperator.MonoDeferredResolution<T, RSocket> {

    final FrameType interactionType;
    final Payload payload;

    MonoFlattingInner(DefaultRSocketClient parent, Payload payload, FrameType interactionType) {
      super(parent);
      this.payload = payload;
      this.interactionType = interactionType;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void accept(RSocket rSocket, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.payload);
        onError(t);
        return;
      }

      Mono<?> source;
      switch (interactionType) {
        case REQUEST_FNF:
          source = rSocket.fireAndForget(this.payload);
          break;
        case REQUEST_RESPONSE:
          source = rSocket.requestResponse(this.payload);
          break;
        case METADATA_PUSH:
          source = rSocket.metadataPush(this.payload);
          break;
        default:
          Operators.error(this.actual, new IllegalStateException("Should never happen"));
          return;
      }

      source.subscribe((CoreSubscriber) this);
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.payload);
      }
    }
  }

  static final class FluxFlattingInner<T>
      extends ResolvingOperator.FluxDeferredResolution<Payload, RSocket> {

    final FrameType interactionType;
    final T fluxOrPayload;

    FluxFlattingInner(DefaultRSocketClient parent, T fluxOrPayload, FrameType interactionType) {
      super(parent);
      this.fluxOrPayload = fluxOrPayload;
      this.interactionType = interactionType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void accept(RSocket rSocket, Throwable t) {
      if (this.requested == STATE_CANCELLED) {
        return;
      }

      if (t != null) {
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
        onError(t);
        return;
      }

      Flux<? extends Payload> source;
      switch (this.interactionType) {
        case REQUEST_STREAM:
          source = rSocket.requestStream((Payload) this.fluxOrPayload);
          break;
        case REQUEST_CHANNEL:
          source = rSocket.requestChannel((Flux<Payload>) this.fluxOrPayload);
          break;
        default:
          Operators.error(this.actual, new IllegalStateException("Should never happen"));
          return;
      }

      source.subscribe(this);
    }

    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
      if (state == STATE_CANCELLED) {
        return;
      }

      if (state == STATE_SUBSCRIBED) {
        this.s.cancel();
      } else {
        this.parent.remove(this);
        ReferenceCountUtil.safeRelease(this.fluxOrPayload);
      }
    }
  }
}
