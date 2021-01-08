package net.xmeter.samplers.mqtt.hivemq;

import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.MqttWebSocketConfig;
import com.hivemq.client.mqtt.MqttWebSocketConfigBuilder;
import com.hivemq.client.mqtt.lifecycle.MqttClientAutoReconnect;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.Mqtt3ClientBuilder;
import com.hivemq.client.mqtt.mqtt3.message.auth.Mqtt3SimpleAuth;
import com.hivemq.client.mqtt.mqtt3.message.auth.Mqtt3SimpleAuthBuilder;
import com.hivemq.client.mqtt.mqtt3.message.connect.Mqtt3ConnectBuilder;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import net.xmeter.samplers.mqtt.ConnectionParameters;
import net.xmeter.samplers.mqtt.MQTTClient;
import net.xmeter.samplers.mqtt.MQTTClientException;
import net.xmeter.samplers.mqtt.MQTTConnection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

class HiveMQTTClient implements MQTTClient {
    private static final Logger logger = Logger.getLogger(HiveMQTTClient.class.getCanonicalName());
    private final ConnectionParameters parameters;
    private final Mqtt3AsyncClient client;

    HiveMQTTClient(ConnectionParameters parameters) {
        this.parameters = parameters;
        Mqtt3ClientBuilder mqtt3ClientBuilder = Mqtt3Client.builder()
                .identifier(parameters.getClientId())
                .serverHost(parameters.getHost())
                .serverPort(parameters.getPort());
        mqtt3ClientBuilder = applyAdditionalConfig(mqtt3ClientBuilder, parameters);
        client = mqtt3ClientBuilder
                .buildAsync();
    }

    private Mqtt3ClientBuilder applyAdditionalConfig(Mqtt3ClientBuilder builder, ConnectionParameters parameters) {
        if (parameters.getReconnectMaxAttempts() > 0) {
            builder = builder.automaticReconnect(MqttClientAutoReconnect.builder().build());
        }
        if (parameters.isSecureProtocol()) {
            MqttClientSslConfig sslConfig = ((HiveMQTTSsl) parameters.getSsl()).getSslConfig();
            builder = builder.sslConfig(sslConfig);
        }
        if (parameters.isWebSocketProtocol()) {
            MqttWebSocketConfigBuilder wsConfigBuilder = MqttWebSocketConfig.builder();
            if (parameters.getPath() != null) {
                wsConfigBuilder = wsConfigBuilder.serverPath(parameters.getPath());
            }
            wsConfigBuilder = wsConfigBuilder.handshakeTimeout(parameters.getConnectTimeout(), TimeUnit.SECONDS);
            builder = builder.webSocketConfig(wsConfigBuilder.build());
        }
        builder = builder.transportConfig()
                .mqttConnectTimeout(parameters.getConnectTimeout(), TimeUnit.SECONDS)
                .applyTransportConfig();
        return builder;
    }

    @Override
    public String getClientId() {
        return parameters.getClientId();
    }

    @Override
    public MQTTConnection connect() throws Exception {
        Mqtt3ConnectBuilder.Send<CompletableFuture<Mqtt3ConnAck>> connectSend = client.connectWith()
                .cleanSession(parameters.isCleanSession())
                .keepAlive(parameters.getKeepAlive());
        Mqtt3SimpleAuth auth = createAuth();
        if (auth != null) {
            connectSend = connectSend.simpleAuth(auth);
        }
        logger.info(() -> "Connect client: " + parameters.getClientId());
        CompletableFuture<Mqtt3ConnAck> connectFuture = connectSend.send();
        try {
            Mqtt3ConnAck connAck = connectFuture.get();
            logger.info(() -> "Connected client: " + parameters.getClientId());
            return new HiveMQTTConnection(client, parameters.getClientId(), connAck);
        } catch (Exception ex) {
            try {
                client.disconnect().get();
            } catch (Exception e) {
                logger.log(Level.FINE, e, () -> "Disconnect on error failed " + client);
            }
            throw new MQTTClientException("Connection error " + client, ex);
        }
    }

    private Mqtt3SimpleAuth createAuth() {
        if (parameters.getUsername() != null) {
            Mqtt3SimpleAuthBuilder.Complete simpleAuth = Mqtt3SimpleAuth.builder()
                    .username(parameters.getUsername());
            if (parameters.getPassword() != null) {
                simpleAuth = simpleAuth.password(parameters.getPassword().getBytes());
            }
            return simpleAuth.build();
        }
        return null;
    }
}
