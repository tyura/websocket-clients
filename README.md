# WebSocketClient
These clients allow real-time tracking of trade data from the MEXC exchange by handling WebSocket communication, including subscribing, receiving, and processing the trade events for different currency pairs.


Build the project.
```
mvn clean install
```

Execute client app.
```
mvn exec:java -Dexec.mainClass="com.mexc.WebSocketClientVerticle"
```