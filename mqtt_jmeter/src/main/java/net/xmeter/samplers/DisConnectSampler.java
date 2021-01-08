package net.xmeter.samplers;

import net.xmeter.samplers.mqtt.MQTTConnection;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DisConnectSampler extends AbstractMQTTSampler {
	private static final long serialVersionUID = 4360869021667126983L;
	private static final Logger logger = Logger.getLogger(DisConnectSampler.class.getCanonicalName());

	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		
		JMeterVariables vars = JMeterContextService.getContext().getVariables();
		MQTTConnection connection = (MQTTConnection) vars.getObject("conn");
		String clientId = (String) vars.getObject("clientId");
		if (connection == null) {
			result.sampleStart();
			result.setSuccessful(false);
			result.setResponseMessage("Connection not found.");
			result.setResponseData("Failed. Connection not found.".getBytes());
			result.setResponseCode("500");
			result.sampleEnd(); // avoid endtime=0 exposed in trace log
			return result;
		}
		
		try {
			result.sampleStart();

			logger.info(() -> "Disconnect connection " + connection);
			connection.disconnect();
			vars.remove("conn"); // clean up thread local var as well
			topicSubscribed.remove(clientId);
			
			result.sampleEnd();
			
			result.setSuccessful(true);
			result.setResponseData("Successful.".getBytes());
			result.setResponseMessage(MessageFormat.format("Connection {0} disconnected.", connection));
			result.setResponseCodeOK();
		} catch (Exception e) {
			logger.log(Level.SEVERE, e, () -> "Failed to disconnect Connection " + connection);
			if (result.getEndTime() == 0) result.sampleEnd(); //avoid re-enter sampleEnd()
			result.setSuccessful(false);
			result.setResponseMessage(MessageFormat.format("Failed to disconnect Connection {0}.", connection));
			result.setResponseData(MessageFormat.format("Client [{0}] failed. Could not disconnect connection.", clientId).getBytes());
			result.setResponseCode("501");
		}
		return result;
	}

}
