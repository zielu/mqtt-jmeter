package net.xmeter.samplers.mqtt;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface MQTTSubListener {
    void accept(ByteBuffer topic, ByteBuffer message, Runnable ack);
}
