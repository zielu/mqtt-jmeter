package net.xmeter.samplers.mqtt;

import java.util.Optional;

public class MQTTPubResult {
    public static MQTTPubResult SUCCESS = new MQTTPubResult(true);

    private final boolean successful;
    private String error;

    public MQTTPubResult(boolean successful) {
        this.successful = successful;
    }

    public MQTTPubResult(boolean successful, String error) {
        this.successful = successful;
        this.error = error;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public Optional<String> getError() {
        return Optional.ofNullable(error);
    }
}
