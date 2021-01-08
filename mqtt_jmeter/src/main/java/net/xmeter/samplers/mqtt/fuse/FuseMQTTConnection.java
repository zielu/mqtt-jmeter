package net.xmeter.samplers.mqtt.fuse;

import net.xmeter.samplers.mqtt.MQTTClientException;
import net.xmeter.samplers.mqtt.MQTTConnection;
import net.xmeter.samplers.mqtt.MQTTPubResult;
import net.xmeter.samplers.mqtt.MQTTQoS;
import net.xmeter.samplers.mqtt.MQTTSubListener;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Listener;
import org.fusesource.mqtt.client.QoS;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

class FuseMQTTConnection implements MQTTConnection {
    private static final Logger logger = Logger.getLogger(FuseMQTTConnection.class.getCanonicalName());

    private final Semaphore disconnectLock = new Semaphore(0);
    private final String clientId;
    private final ConnectionCallback connectionCallback;
    private final CallbackConnection callbackConnection;

    private final Listener nullListener = new Listener() {
        @Override
        public void onConnected() {
            logger.fine(() -> "Connected client: " + clientId);
        }

        @Override
        public void onDisconnected() {
            logger.fine(() -> "Disconnected client: " + clientId);
        }

        @Override
        public void onPublish(UTF8Buffer utf8Buffer, Buffer buffer, Runnable runnable) {

        }

        @Override
        public void onFailure(Throwable throwable) {
            logger.log(Level.SEVERE, "Sub listener failure", throwable);
        }
    };

    FuseMQTTConnection(String clientId, ConnectionCallback connectionCallback, CallbackConnection callbackConnection) {
        this.clientId = clientId;
        this.connectionCallback = connectionCallback;
        this.callbackConnection = callbackConnection;
    }

    @Override
    public boolean isConnectionSucc() {
        return connectionCallback.isConnSucc();
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void disconnect() throws MQTTClientException {
        callbackConnection.disconnect(new Callback<Void>() {
            @Override
            public void onSuccess(Void aVoid) {
                disconnectLock.release();
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.log(Level.SEVERE, "Disconnect failed", throwable);
                disconnectLock.release();
            }
        });
        try {
            disconnectLock.tryAcquire(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MQTTClientException("Disconnect timeout", e);
        }
    }

    @Override
    public CompletionStage<MQTTPubResult> publish(String topicName, byte[] message, MQTTQoS qos, boolean retained) {
        final Object pubLock = new Object();
        QoS fuseQos = FuseUtil.map(qos);
        PubCallback pubCallback = new PubCallback(pubLock, fuseQos);

        try {
            if (qos == MQTTQoS.AT_MOST_ONCE) {
                //For QoS == 0, the callback is the same thread with sampler thread, so it cannot use the lock object wait() & notify() in else block;
                //Otherwise the sampler thread will be blocked.
                callbackConnection.publish(topicName, message, fuseQos, retained, pubCallback);
            } else {
                synchronized (pubLock) {
                    callbackConnection.publish(topicName, message, fuseQos, retained, pubCallback);
                    pubLock.wait();
                }
            }
            return CompletableFuture.completedFuture(new MQTTPubResult(pubCallback.isSuccessful(), pubCallback.getErrorMessage()));
        } catch (Exception exception) {
            return CompletableFuture.completedFuture(new MQTTPubResult(false, exception.getMessage()));
        }
    }

    @Override
    public void subscribe(String[] topicNames, MQTTQoS qos, Runnable onSuccess, Consumer<Throwable> onFailure) {
        FuseSubscription subscription = new FuseSubscription(topicNames, qos, callbackConnection);
        subscription.subscribe(onSuccess, onFailure);
    }

    @Override
    public void setSubListener(MQTTSubListener listener) {
        if (listener == null) {
            callbackConnection.listener(nullListener);
        } else {
            callbackConnection.listener(new Listener() {
                @Override
                public void onPublish(UTF8Buffer topic, Buffer body, Runnable ack) {
                    listener.accept(topic.toByteBuffer(), body.toByteBuffer(), ack);
                }

                @Override
                public void onFailure(Throwable value) {
                    logger.log(Level.SEVERE, "Sub listener failure", value);
                }

                @Override
                public void onDisconnected() {
                    logger.fine(() -> "Disconnected client: " + clientId);
                }

                @Override
                public void onConnected() {
                    logger.fine(() -> "Connected client: " + clientId);
                }
            });
        }
    }

    @Override
    public String toString() {
        return "FuseMQTTConnection{" +
                "clientId='" + clientId + '\'' +
                '}';
    }
}
