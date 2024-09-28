package com.mexc;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MexcWebSocketClient {

    private static final List<JsonNode> MESSAGES = new CopyOnWriteArrayList<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    public static void main(String[] args) throws Exception {
        List<String> symbols = getCurrencyPairs();
        System.out.println("Total number of symbols in Mexc: " + symbols.size());

        int nSymbols = 100;
        int batch = 15;

        for (int i = 0; i < nSymbols; i += batch) {
            List<String> curSymbols = symbols.subList(i, Math.min(i + batch, symbols.size()));
            executor.execute(() -> threadTrades(curSymbols));
        }

        executor.execute(MexcWebSocketClient::threadMonitor);
    }

    public static List<String> getCurrencyPairs() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI("https://api.mexc.com/api/v3/ticker/bookTicker"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode rootNode = OBJECT_MAPPER.readTree(response.body());

        List<String> symbols = new ArrayList<>();
        for (JsonNode node : rootNode) {
            symbols.add(node.get("symbol").asText());
        }
        return symbols;
    }

    public static void threadTrades(List<String> symbols) {
        try {
            WebSocketClientListener listener = new WebSocketClientListener(symbols);
            WebSocket webSocket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .buildAsync(URI.create("wss://wbs.mexc.com/ws"), listener)
                    .join();

            listener.setWebSocket(webSocket);
            listener.subscribe();
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

    static class WebSocketClientListener implements Listener {

        private final List<String> symbols;
        private WebSocket webSocket;

        public WebSocketClientListener(List<String> symbols) {
            this.symbols = symbols;
        }

        public void setWebSocket(WebSocket webSocket) {
            this.webSocket = webSocket;
        }

        public void subscribe() {
            List<String> args = new ArrayList<>();
            for (String symbol : symbols) {
                args.add("spot@public.deals.v3.api@" + symbol);
            }

            String subscribeMessage = OBJECT_MAPPER.createObjectNode()
                    .put("method", "SUBSCRIPTION")
                    .set("params", OBJECT_MAPPER.valueToTree(args))
                    .toString();

            webSocket.sendText(subscribeMessage, true);
            startPingRoutine();
        }

        private void startPingRoutine() {
            executor.scheduleAtFixedRate(() -> {
                if (webSocket != null) {
                    String pingMessage = OBJECT_MAPPER.createObjectNode()
                            .put("method", "PING")
                            .toString();
                    webSocket.sendText(pingMessage, true);
                }
            }, 0, 3, TimeUnit.SECONDS);
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            System.out.println("WebSocket connected");
            Listener.super.onOpen(webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            try {
                JsonNode message = OBJECT_MAPPER.readTree(data.toString());
                MESSAGES.add(message);
                System.out.println("New message: " + message);
            } catch (Exception e) {
                e.printStackTrace();
            }
            webSocket.request(1);
            return Listener.super.onText(webSocket, data, last);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            error.printStackTrace();
            Listener.super.onError(webSocket, error);
        }
    }
}
