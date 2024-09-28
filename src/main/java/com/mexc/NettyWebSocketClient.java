package com.mexc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.CharsetUtil;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;

public class NettyWebSocketClient {

    private static final List<String> MESSAGES = new CopyOnWriteArrayList<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        List<String> symbols = getCurrencyPairs();
        System.out.println("Total number of symbols in MEXC: " + symbols.size());

        int nSymbols = 100;
        int batch = 15;

        for (int i = 0; i < nSymbols; i += batch) {
            List<String> curSymbols = symbols.subList(i, Math.min(i + batch, symbols.size()));
            new Thread(() -> threadTrades(curSymbols)).start();
        }

        new Thread(NettyWebSocketClient::threadMonitor).start();
    }

    public static List<String> getCurrencyPairs() throws Exception {
        String url = "https://api.mexc.com/api/v3/ticker/bookTicker";
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");

        int responseCode = conn.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(content.toString());

            List<String> symbols = new ArrayList<>();
            for (JsonNode node : rootNode) {
                String symbol = node.get("symbol").asText();
                symbols.add(symbol);
            }
            return symbols;
        } else {
            throw new RuntimeException("Failed to get currency pairs: HTTP error code : " + responseCode);
        }
    }

    public static void threadTrades(List<String> symbols) {
        try {
            URI uri = new URI("wss://wbs.mexc.com/ws");
            EventLoopGroup group = new NioEventLoopGroup();
            final SslContext sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            try {
                final WebSocketClientHandler handler = new WebSocketClientHandler(uri, symbols);

                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(group)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .option(ChannelOption.TCP_NODELAY, true)
                        // .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(
                                        sslCtx.newHandler(ch.alloc(), uri.getHost(), 443),
                                        new HttpClientCodec(),
                                        new HttpObjectAggregator(8192),
                                        WebSocketClientCompressionHandler.INSTANCE,
                                        handler,
                                        new IdleStateHandler(0, 0, 3, TimeUnit.SECONDS));
                            }
                        });
                System.out.println(uri.getPort());
                Channel channel = bootstrap.connect(uri.getHost(), 443).sync().channel();
                handler.handshakeFuture().sync();
            } finally {
                group.shutdownGracefully();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void threadMonitor() {
        while (true) {
            System.out.println("Number of messages: " + MESSAGES.size());
            MESSAGES.clear();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class WebSocketClientHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;
        private final List<String> symbols;

        WebSocketClientHandler(URI uri, List<String> symbols) {
            this.symbols = symbols;
            this.handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
        }

        public ChannelFuture handshakeFuture() {
            return handshakeFuture;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
            subscribe(ctx, symbols);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("WebSocket Client disconnected!");
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) {

            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    System.out.println("WebSocket Client connected!");
                    handshakeFuture.setSuccess();
                } catch (WebSocketHandshakeException e) {
                    System.out.println("WebSocket Client failed to connect");
                    handshakeFuture.setFailure(e);
                }
                return;
            }

            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                throw new IllegalStateException(
                        "Unexpected FullHttpResponse (getStatus=" + response.status() +
                                ", content=" + response.content().toString(CharsetUtil.UTF_8) + ')');
            }

            String message = msg.text();
            try {
                Object res = OBJECT_MAPPER.readValue(message, Object.class);
                MESSAGES.add(res.toString());
                System.out.println("New message: " + res);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }

        public void sendPing(ChannelHandlerContext ctx) {
            try {
                String pingMessage = OBJECT_MAPPER.writeValueAsString(new PingMessage("PING"));
                ctx.channel().writeAndFlush(new TextWebSocketFrame(pingMessage));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void subscribe(ChannelHandlerContext ctx, List<String> symbols) {
            try {
                List<String> args = new ArrayList<>();
                for (String symbol : symbols) {
                    args.add("spot@public.deals.v3.api@" + symbol);
                }
                SubscriptionMessage subscriptionMessage = new SubscriptionMessage("SUBSCRIPTION", args);
                String message = OBJECT_MAPPER.writeValueAsString(subscriptionMessage);
                ctx.channel().writeAndFlush(new TextWebSocketFrame(message));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                sendPing(ctx);
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        static class PingMessage {
            public String method;

            public PingMessage(String method) {
                this.method = method;
            }
        }

        static class SubscriptionMessage {
            public String method;
            public List<String> params;

            public SubscriptionMessage(String method, List<String> params) {
                this.method = method;
                this.params = params;
            }
        }
    }
}
