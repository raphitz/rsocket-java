package io.rsocket.examples.transport.tcp.client;

import io.rsocket.Payload;
import io.rsocket.RSocketClient;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RSocketClientExample {
  static final Logger logger = LoggerFactory.getLogger(RSocketClientExample.class);

  public static void main(String[] args) {

    RSocketServer.create(
            SocketAcceptor.forRequestResponse(
                p -> {
                  String data = p.getDataUtf8();
                  logger.info("Received request data {}", data);

                  Payload responsePayload = DefaultPayload.create("Echo: " + data);
                  p.release();

                  return Mono.just(responsePayload);
                }))
        .bindNow(TcpServerTransport.create("localhost", 7000));

    RSocketClient rSocketClient =
        RSocketConnector.create().connectAsClient(TcpClientTransport.create("localhost", 7000));

    rSocketClient
        .requestResponse(DefaultPayload.create("Test Request"))
        .doOnNext(
            d -> {
              logger.info("Received response data {}", d.getDataUtf8());
              d.release();
            })
        .block();
  }
}
