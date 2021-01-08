package net.xmeter.samplers;

import net.xmeter.SubBean;
import net.xmeter.samplers.mqtt.MQTTClientRuntimeException;
import net.xmeter.samplers.mqtt.MQTTConnection;
import net.xmeter.samplers.mqtt.MQTTQoS;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("deprecation")
public class SubSampler extends AbstractMQTTSampler {
	private static final long serialVersionUID = 2979978053740194951L;
	private static final Logger logger = Logger.getLogger(SubSampler.class.getCanonicalName());

	private static final Charset charset = StandardCharsets.UTF_8;
	private final transient CharsetDecoder decoder = charset.newDecoder();

	private final transient Deque<SubBean> batches = new ConcurrentLinkedDeque<>();
	private transient MQTTConnection connection = null;
	private transient String clientId;
	private boolean subFailed = false;

	private boolean sampleByTime = true; // initial values
	private int sampleElapsedTime = 1000;
	private int sampleCount = 1;

	private boolean printFlag = false;

	private final transient Object dataLock = new Object();

	public String getQOS() {
		return getPropertyAsString(QOS_LEVEL, String.valueOf(QOS_0));
	}

	public void setQOS(String qos) {
		setProperty(QOS_LEVEL, qos);
	}

	public String getTopics() {
		return getPropertyAsString(TOPIC_NAME, DEFAULT_TOPIC_NAME);
	}

	public void setTopics(String topicsName) {
		setProperty(TOPIC_NAME, topicsName);
	}
	
	public String getSampleCondition() {
		return getPropertyAsString(SAMPLE_CONDITION, SAMPLE_ON_CONDITION_OPTION1);
	}
	
	public void setSampleCondition(String option) {
		setProperty(SAMPLE_CONDITION, option);
	}
	
	public String getSampleCount() {
		return getPropertyAsString(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_COUNT);
	}
	
	public void setSampleCount(String count) {
		setProperty(SAMPLE_CONDITION_VALUE, count);
	}
	
	public String getSampleElapsedTime() {
		return getPropertyAsString(SAMPLE_CONDITION_VALUE, DEFAULT_SAMPLE_VALUE_ELAPSED_TIME_MILLI_SEC);
	}
	
	public void setSampleElapsedTime(String elapsedTime) {
		setProperty(SAMPLE_CONDITION_VALUE, elapsedTime);
	}

	public boolean isAddTimestamp() {
		return getPropertyAsBoolean(ADD_TIMESTAMP);
	}

	public void setAddTimestamp(boolean addTimestamp) {
		setProperty(ADD_TIMESTAMP, addTimestamp);
	}

	public boolean isDebugResponse() {
		return getPropertyAsBoolean(DEBUG_RESPONSE, false);
	}

	public void setDebugResponse(boolean debugResponse) {
		setProperty(DEBUG_RESPONSE, debugResponse);
	}

	@Override
	public SampleResult sample(Entry arg0) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
	
		JMeterVariables vars = JMeterContextService.getContext().getVariables();
		connection = (MQTTConnection) vars.getObject("conn");
		clientId = (String) vars.getObject("clientId");
		if (connection == null) {
			return fillFailedResult(result, "500", "Subscribe failed because connection is not established.");
		}
		
		sampleByTime = SAMPLE_ON_CONDITION_OPTION1.equals(getSampleCondition());
		try {
			if (sampleByTime) {
				sampleElapsedTime = Integer.parseInt(getSampleElapsedTime());
			} else {
				sampleCount = Integer.parseInt(getSampleCount());
			}
		} catch (NumberFormatException e) {
			return fillFailedResult(result, "510", "Unrecognized value for sample elapsed time or message count.");
		}
		
		if (sampleByTime && sampleElapsedTime <=0 ) {
			return fillFailedResult(result, "511", "Sample on elapsed time: must be greater than 0 ms.");
		} else if (sampleCount < 1) {
			return fillFailedResult(result, "512", "Sample on message count: must be greater than 1.");
		}
		
		final String topicsName= getTopics();
		Set<String> topics = topicSubscribed.get(clientId);
		if (topics == null) {
			if (logger.isLoggable(Level.SEVERE)) {
				logger.severe("subscribed topics haven't been initiated. [clientId: " + (clientId == null ? "null" : clientId) + "]");
			}
			topics = new HashSet<>();
			topics.add(topicsName);
			topicSubscribed.put(clientId, topics);
			listenToTopics(topicsName);  // TODO: run once or multiple times ?
		} else {
			if (!topics.contains(topicsName)) {
				topics.add(topicsName);
				topicSubscribed.put(clientId, topics);
				logger.fine(() -> "Listen to topics: " + topicsName);
				listenToTopics(topicsName);  // TODO: run once or multiple times ?
			}
		}
		
		if (subFailed) {
			return fillFailedResult(result, "501", "Failed to subscribe to topic(s):" + topicsName);
		}

		SubBean bean = null;
		if(sampleByTime) {
			try {
				TimeUnit.MILLISECONDS.sleep(sampleElapsedTime);
			} catch (InterruptedException e) {
				logger.log(Level.INFO, "Received exception when waiting for notification signal", e);
				Thread.currentThread().interrupt();
			}
			synchronized (dataLock) {
				bean = batches.poll();
			}
		} else {
			synchronized (dataLock) {
				while (needWaitForSampleCount()) {
					try {
						dataLock.wait();
						bean = batches.poll();
					} catch (InterruptedException e) {
						logger.log(Level.INFO, "Received exception when waiting for notification signal", e);
						Thread.currentThread().interrupt();
					}
				}
			}
		}
		resetListener();
		result.sampleStart();
		produceResult(bean, result, topicsName);
		return result;
	}

	private boolean needWaitForSampleCount() {
		int receivedCount = (batches.isEmpty() ? 0 : batches.element().getReceivedCount());
		return receivedCount < sampleCount;
	}
	
	private void produceResult(SubBean bean, SampleResult result, String topicName) {
		if(bean == null) { // In "elapsed time" mode, return "dummy" when time is reached
			bean = new SubBean();
		}
		int receivedCount = bean.getReceivedCount();
		String message = MessageFormat.format("Received {0} of message.", receivedCount);
		String content = "";
		if (isDebugResponse()) {
			StringBuilder contentValue = new StringBuilder();
			for (String item : bean.getContents()) {
				contentValue.append(item).append("\n");
			}
			content = contentValue.toString();
		}
		result = fillOKResult(result, bean.getReceivedMessageSize(), message, content);
		if (logger.isLoggable(Level.FINE)) {
			logger.fine("sub [topic]: " + topicName + ", [payload]: " + content);
		}
		
		if(receivedCount == 0) {
			result.setEndTime(result.getStartTime()); // dummy result, rectify sample time
		} else {
			if (isAddTimestamp()) {
				result.setEndTime(result.getStartTime() + (long) bean.getAvgElapsedTime()); // rectify sample time
				result.setLatency((long) bean.getAvgElapsedTime());
			} else {
				result.setEndTime(result.getStartTime()); // received messages w/o timestamp, then we cannot reliably calculate elapsed time
			}
		}
		result.setSampleCount(receivedCount);
	}
	
	private void listenToTopics(final String topicsName) {
		setListener(sampleByTime, sampleCount);
		int qos;
		try {
			qos = Integer.parseInt(getQOS());
		} catch(Exception ex) {
			logger.log(Level.SEVERE, ex, () -> MessageFormat.format("Specified invalid QoS value {0}, set to default QoS value {1}!", ex.getMessage(), QOS_0));
			qos = QOS_0;
		}
		
		final String[] paraTopics = topicsName.split(",");
		if(qos < 0 || qos > 2) {
			if (logger.isLoggable(Level.SEVERE)) {
				logger.severe("Specified invalid QoS value, set to default QoS value " + qos);
			}
			qos = QOS_0;
		}
		if (paraTopics.length == 0) {
			logger.severe("Specified empty topics list");
			subFailed = true;
		} else {
			connection.subscribe(paraTopics, MQTTQoS.fromValue(qos), () -> {
				logger.fine(() -> "sub successful, topic length is " + paraTopics.length);
			}, error -> {
				logger.log(Level.INFO, "subscribe failed", error);
				subFailed = true;
			});
		}
	}
	
	private void setListener(final boolean sampleByTime, final int sampleCount) {
		connection.setSubListener(((topic, message, ack) -> {
			ack.run();

			if(sampleByTime) {
				synchronized (dataLock) {
					handleSubBean(sampleByTime, message, sampleCount);
				}
			} else {
				synchronized (dataLock) {
					SubBean bean = handleSubBean(sampleByTime, message, sampleCount);
					if(bean.getReceivedCount() == sampleCount) {
						dataLock.notifyAll();
					}
				}
			}
		}));
	}

	private void resetListener() {
		connection.setSubListener(null);
	}
	
	private SubBean handleSubBean(boolean sampleByTime, ByteBuffer msg, int sampleCount) {
		SubBean bean;
		if(batches.isEmpty()) {
			bean = new SubBean();
			batches.add(bean);
		} else {
			bean = batches.peekLast();
		}

		int receivedCount = bean.getReceivedCount();
		if((!sampleByTime) && (receivedCount == sampleCount)) { //Create a new batch when latest bean is full.
			logger.info("The tail bean is full, will create a new bean for it.");
			bean = new SubBean();
			batches.add(bean);
		}
		if (isAddTimestamp()) {
			String message = decode(msg);
			int index = message.indexOf(TIME_STAMP_SEP_FLAG);
			if (index == -1 && (!printFlag)) {
				logger.info(() -> "Payload does not include timestamp: " + message);
				printFlag = true;
			} else if (index != -1) {
				long start = Long.parseLong(message.substring(0, index));
				long elapsed = System.currentTimeMillis() - start;
				
				double avgElapsedTime = bean.getAvgElapsedTime();
				avgElapsedTime = (avgElapsedTime * receivedCount + elapsed) / (receivedCount + 1);
				bean.setAvgElapsedTime(avgElapsedTime);
			}
		}
		if (isDebugResponse()) {
			bean.getContents().add(decode(msg));
		}
		bean.setReceivedMessageSize(bean.getReceivedMessageSize() + msg.limit());
		bean.setReceivedCount(receivedCount + 1);
		return bean;
	}

	private String decode(ByteBuffer value) {
		try {
			return decoder.decode(value).toString();
		} catch (CharacterCodingException e) {
			throw new MQTTClientRuntimeException("Failed to decode", e);
		}
	}

	private SampleResult fillFailedResult(SampleResult result, String code, String message) {
		result.sampleStart();
		result.setResponseCode(code); // 5xx means various failures
		result.setSuccessful(false);
		result.setResponseMessage(message);
		if (clientId != null) {
			result.setResponseData(MessageFormat.format("Client [{0}]: {1}", clientId, message).getBytes());
		} else {
			result.setResponseData(message.getBytes());
		}
		result.sampleEnd();
		
		// avoid massive repeated "early stage" failures in a short period of time
		// which probably overloads JMeter CPU and distorts test metrics such as TPS, avg response time
		try {
			TimeUnit.MILLISECONDS.sleep(SUB_FAIL_PENALTY);
		} catch (InterruptedException e) {
			logger.log(Level.INFO, "Received exception when waiting for notification signal", e);
			Thread.currentThread().interrupt();
		}
		return result;
	}

	private SampleResult fillOKResult(SampleResult result, int size, String message, String contents) {
		result.setResponseCode("200");
		result.setSuccessful(true);
		result.setResponseMessage(message);
		result.setBodySize(size);
		result.setBytes(size);
		result.setResponseData(contents.getBytes());
		result.sampleEnd();
		return result;
	}

}
