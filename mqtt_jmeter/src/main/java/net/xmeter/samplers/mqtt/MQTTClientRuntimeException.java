package net.xmeter.samplers.mqtt;

public class MQTTClientRuntimeException extends RuntimeException {
    public MQTTClientRuntimeException(String message) {
        super(message);
    }

    public MQTTClientRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
