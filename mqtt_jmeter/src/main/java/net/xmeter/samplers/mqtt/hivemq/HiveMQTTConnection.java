package net.xmeter.samplers.mqtt.hivemq;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3SubscribeBuilder;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscription;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAckReturnCode;
import net.xmeter.samplers.mqtt.MQTTClientRuntimeException;
import net.xmeter.samplers.mqtt.MQTTConnection;
import net.xmeter.samplers.mqtt.MQTTPubResult;
import net.xmeter.samplers.mqtt.MQTTQoS;
import net.xmeter.samplers.mqtt.MQTTSubListener;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

class HiveMQTTConnection implements MQTTConnection {
    private static final Logger logger = Logger.getLogger(HiveMQTTConnection.class.getCanonicalName());
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static final Runnable AUTO_ACKNOWLEDGE = () -> {};

    private final Mqtt3AsyncClient client;
    private final String clientId;
    private final Mqtt3ConnAck connAck;
    private final AtomicReference<MQTTSubListener> listener = new AtomicReference<>();

    HiveMQTTConnection(Mqtt3AsyncClient client, String clientId, Mqtt3ConnAck connAck) {
        this.client = client;
        this.clientId = clientId;
        this.connAck = connAck;
    }

    @Override
    public boolean isConnectionSucc() {
        return !connAck.getReturnCode().isError();
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void disconnect() {
        client.disconnect();
    }

    @Override
    public CompletionStage<MQTTPubResult> publish(String topicName, byte[] message, MQTTQoS qoS, boolean retained) {
        return client.publishWith()
                .topic(topicName)
                .payload(message)
                .qos(HiveUtil.map(qoS))
                .retain(retained)
                .send()
                .handle((publish, error) -> {
                    if (error != null) {
                        return new MQTTPubResult(false, error.getMessage());
                    } else {
                        return MQTTPubResult.SUCCESS;
                    }
                });
    }

    @Override
    public void subscribe(String[] topicNames, MQTTQoS qos, Runnable onSuccess, Consumer<Throwable> onFailure) {
        if (topicNames.length == 0) {
            throw new MQTTClientRuntimeException("Topics names are empty");
        }
        MqttQos hiveQos = HiveUtil.map(qos);
        Mqtt3Subscribe subscribe = null;
        Mqtt3SubscribeBuilder builder = Mqtt3Subscribe.builder();
        for (int i = 0; i < topicNames.length; i++) {
            String topicName = topicNames[i];
            Mqtt3Subscription subscription = Mqtt3Subscription.builder().topicFilter(topicName).qos(hiveQos).build();
            if (i < topicNames.length - 1) {
                builder = builder.addSubscription(subscription);
            } else {
                subscribe = builder.addSubscription(subscription).build();
            }
        }

        client.subscribe(subscribe, this::handlePublishReceived).whenComplete((ack, error) -> {
            List<Mqtt3SubAckReturnCode> ackCodes = ack.getReturnCodes();
            for (int i = 0; i < ackCodes.size(); i++) {
                Mqtt3SubAckReturnCode ackCode = ackCodes.get(i);
                if (ackCode.isError()) {
                    int index = i;
                    logger.warning(() -> "Failed to subscribe " + topicNames[index] + " code: " + ackCode.name());
                }
            }
            if (error != null) {
                onFailure.accept(error);
            } else {
                onSuccess.run();
            }
        });
    }

    private void handlePublishReceived(Mqtt3Publish received) {
        MQTTSubListener currentListener = this.listener.get();
        if (currentListener != null) {
            currentListener.accept(received.getTopic().toByteBuffer(), received.getPayload().orElse(EMPTY_BUFFER), AUTO_ACKNOWLEDGE);
        }
    }

    @Override
    public void setSubListener(MQTTSubListener listener) {
        this.listener.set(listener);
    }

    @Override
    public String toString() {
        return "HiveMQTTConnection{" +
                "clientId='" + clientId + '\'' +
                '}';
    }
}
