package com.mexc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import com.fasterxml.jackson.databind.JsonNode;

public class WebSocketClientVerticle extends AbstractVerticle {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<JsonObject> messages = new ArrayList<>();

    @Override
    public void start() throws Exception {
        // Fetch currency pairs
        List<String> symbols = getCurrencyPairs();
        System.out.println("Total number of symbols in mexc: " + symbols.size());

        int nSymbols = 100;
        int batch = 15;

        for (int i = 0; i < nSymbols; i += batch) {
            List<String> curSymbols = symbols.subList(i, Math.min(i + batch, symbols.size()));
            startTradeThread(curSymbols);
        }

        startMonitorThread();
    }

    private List<String> getCurrencyPairs() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI("https://api.mexc.com/api/v3/ticker/bookTicker"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        JsonNode rootNode = objectMapper.readTree(response.body());

        List<String> symbols = new ArrayList<>();
        for (JsonNode node : rootNode) {
            symbols.add(node.get("symbol").asText());
        }
        return symbols;
    }

    private void startTradeThread(List<String> symbols) {
        vertx.executeBlocking(promise -> {
            HttpClientOptions clientOptions = new HttpClientOptions().setSsl(true).setTrustAll(true);

            WebSocketConnectOptions wsOptions = new WebSocketConnectOptions()
                    .setHost("wbs.mexc.com")
                    .setPort(443)
                    .setURI("/ws");

            vertx.createHttpClient(clientOptions).webSocket(wsOptions, res -> {
                if (res.succeeded()) {
                    WebSocket webSocket = res.result();
                    System.out.println("WebSocket connected for symbols: " + symbols);

                    String[] args = symbols.stream()
                            .map(symbol -> "spot@public.deals.v3.api@" + symbol)
                            .toArray(String[]::new);

                    Map<String, Object> messageMap = new HashMap<>();
                    messageMap.put("method", "SUBSCRIPTION");
                    messageMap.put("params", args);

                    try {
                        String subscriptionMessage = objectMapper.writeValueAsString(messageMap);
                        webSocket.writeTextMessage(subscriptionMessage);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }

                    webSocket.handler(message -> {
                        JsonObject jsonMessage = new JsonObject(message.toString());
                        messages.add(jsonMessage);
                        System.out.println("New message: " + jsonMessage);
                    });

                    webSocket.closeHandler(v -> {
                        System.out.println("WebSocket closed for symbols: " + symbols);
                    });

                } else {
                    System.err.println("Failed to connect WebSocket: " + res.cause().getMessage());
                }
            });

            promise.complete();
        }, false, res -> {
            if (res.failed()) {
                System.err.println("Failed to start trade thread: " + res.cause().getMessage());
            }
        });
    }

    private void startMonitorThread() {
        vertx.setPeriodic(5000, id -> {
            System.out.println("Number of messages: " + messages.size());
            messages.clear();
        });
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new WebSocketClientVerticle());
    }
}
