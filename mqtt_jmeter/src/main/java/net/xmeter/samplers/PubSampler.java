package net.xmeter.samplers;

import net.xmeter.Util;
import net.xmeter.samplers.mqtt.MQTTConnection;
import net.xmeter.samplers.mqtt.MQTTPubResult;
import net.xmeter.samplers.mqtt.MQTTQoS;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import javax.xml.bind.DatatypeConverter;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSampler extends AbstractMQTTSampler {
	private static final long serialVersionUID = 4312341622759500786L;
	private static final Logger logger = Logger.getLogger(PubSampler.class.getCanonicalName());
	private static final byte[] PUBLISH_SUCCESS = "Publish successfully.".getBytes();
	private static final byte[] FAILED_NO_CONNECTION = "Publish failed because connection is not established.".getBytes();
	private static final byte[] EMPTY_BYTES = new byte[0];

	private transient MQTTConnection connection = null;
	private String payload = null;
	private String topicName = "";

	public String getQOS() {
		return getPropertyAsString(QOS_LEVEL, String.valueOf(QOS_0));
	}

	public void setQOS(String qos) {
		setProperty(QOS_LEVEL, qos);
	}

	public String getTopic() {
		return getPropertyAsString(TOPIC_NAME, DEFAULT_TOPIC_NAME);
	}

	public void setTopic(String topicName) {
		setProperty(TOPIC_NAME, topicName);
	}

	public boolean isAddTimestamp() {
		return getPropertyAsBoolean(ADD_TIMESTAMP);
	}

	public void setAddTimestamp(boolean addTimestamp) {
		setProperty(ADD_TIMESTAMP, addTimestamp);
	}

	public String getMessageType() {
		return getPropertyAsString(MESSAGE_TYPE, MESSAGE_TYPE_RANDOM_STR_WITH_FIX_LEN);
	}

	public void setMessageType(String messageType) {
		setProperty(MESSAGE_TYPE, messageType);
	}

	public String getMessageLength() {
		return getPropertyAsString(MESSAGE_FIX_LENGTH, DEFAULT_MESSAGE_FIX_LENGTH);
	}

	public void setMessageLength(String length) {
		setProperty(MESSAGE_FIX_LENGTH, length);
	}

	public String getMessage() {
		return getPropertyAsString(MESSAGE_TO_BE_SENT, "");
	}

	public void setMessage(String message) {
		setProperty(MESSAGE_TO_BE_SENT, message);
	}
	
	public void setRetainedMessage(Boolean retained) {
		setProperty(RETAINED_MESSAGE, retained);
	}
	
	public boolean getRetainedMessage() {
		return getPropertyAsBoolean(RETAINED_MESSAGE, false);
	}

	public static byte[] hexToBinary(String hex) {
	    return DatatypeConverter.parseHexBinary(hex);
	}
	
	@Override
	public SampleResult sample(Entry arg0) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
	
		JMeterVariables vars = JMeterContextService.getContext().getVariables();
		connection = (MQTTConnection) vars.getObject("conn");
		String clientId = (String) vars.getObject("clientId");
		if (connection == null) {
			result.sampleStart();
			result.setSuccessful(false);
			result.setResponseMessage("Publish: Connection not found.");
			result.setResponseData(FAILED_NO_CONNECTION);
			result.setResponseCode("500");
			result.sampleEnd(); // avoid endtime=0 exposed in trace log
			return result;
		}

		byte[] toSend = EMPTY_BYTES;
		try {
			byte[] tmp = EMPTY_BYTES;

			if (MESSAGE_TYPE_HEX_STRING.equals(getMessageType())) {
				tmp = hexToBinary(getMessage());
			} else if (MESSAGE_TYPE_STRING.equals(getMessageType())) {
				tmp = getMessage().getBytes();
			} else if(MESSAGE_TYPE_RANDOM_STR_WITH_FIX_LEN.equals(getMessageType())) {
				if (payload == null) {
					payload = Util.generatePayload(Integer.parseInt(getMessageLength()));
				}
				tmp = payload.getBytes();
			}

			topicName = getTopic();
			boolean retainedMsg = getRetainedMessage();
			if (isAddTimestamp()) {
				byte[] timePrefix = (System.currentTimeMillis() + TIME_STAMP_SEP_FLAG).getBytes();
				toSend = new byte[timePrefix.length + tmp.length];
				System.arraycopy(timePrefix, 0, toSend, 0, timePrefix.length);
				System.arraycopy(tmp, 0, toSend, timePrefix.length , tmp.length);
			} else {
				toSend = new byte[tmp.length];
				System.arraycopy(tmp, 0, toSend, 0 , tmp.length);
			}
			
			result.sampleStart();
			if (logger.isLoggable(Level.FINE)) {
				logger.fine("pub [topic]: " + topicName + ", [payload]: " + new String(toSend));
			}
			final byte[] finalToSend = toSend;
			return connection.publish(topicName, finalToSend, getMqttQos(), retainedMsg)
					.thenApply(pubResult -> onPublish(clientId, finalToSend, result, pubResult))
					.toCompletableFuture()
					.get();
		} catch (Exception ex) {
			return onError(clientId, toSend, result, ex);
		}
	}

	private MQTTQoS getMqttQos() {
		int qos = Integer.parseInt(getQOS());
		switch (qos) {
			case 1:
				return MQTTQoS.AT_LEAST_ONCE;
			case 2:
				return MQTTQoS.EXACTLY_ONCE;
			default:
				return MQTTQoS.AT_MOST_ONCE;
		}
	}

	private SampleResult onPublish(String clientId, byte[] toSend, SampleResult result, MQTTPubResult pubResult) {
		result.sampleEnd();
		result.setSamplerData(new String(toSend));
		result.setSentBytes(toSend.length);
		result.setLatency(result.getEndTime() - result.getStartTime());
		result.setSuccessful(pubResult.isSuccessful());

		if(pubResult.isSuccessful()) {
			result.setResponseData(PUBLISH_SUCCESS);
			result.setResponseMessage(MessageFormat.format("publish successfully for Connection {0}.", connection));
			result.setResponseCodeOK();
		} else {
			result.setSuccessful(false);
			result.setResponseMessage(MessageFormat.format("Publish failed for connection {0}.", connection));
			result.setResponseData(MessageFormat.format("Client [{0}] publish failed: {1}", clientId,
					pubResult.getError().orElse("")).getBytes());
			result.setResponseCode("501");
			if (logger.isLoggable(Level.WARNING)) {
				logger.warning(
						MessageFormat.format("** [clientId: {0}, topic: {1}, payload: {2}] Publish failed for connection {3}.",
								clientId,
								topicName, new String(toSend), connection));
			}
			pubResult.getError().ifPresent(logger::info);
		}
		return result;
	}

	private SampleResult onError(String clientId, byte[] toSend, SampleResult result, Exception ex) {
		logger.log(Level.SEVERE, ex, () -> "Publish failed for connection " + connection);
		if (result.getEndTime() == 0) result.sampleEnd();
		result.setLatency(result.getEndTime() - result.getStartTime());
		result.setSuccessful(false);
		result.setResponseMessage(MessageFormat.format("Publish failed for connection {0}.", connection));
		result.setResponseData(MessageFormat.format("Client [{0}] publish failed: {1}", clientId, ex.getMessage()).getBytes());
		result.setResponseCode("502");
		if (logger.isLoggable(Level.WARNING)) {
			logger.warning(
					MessageFormat.format("** [clientId: {0}, topic: {1}, payload: {2}] Publish failed for connection {3}.",
							clientId, topicName, new String(toSend), connection));
		}
		return result;
	}
}
