package gov.usgs.wma.waterdata.groundwater;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.Topic;

/**
 * Manager class for SNS actions.
 */
@Component
public class SnsUtil {
	private static final String TOPIC_BASE_NAME = "aqts-capture-etl-rdb";
	private final AmazonSNS snsClient = AmazonSNSClientBuilder.defaultClient();
	private final Topic snsTopic = getSNSTopic();

	@Autowired
	private Properties properties;

	SnsUtil(Properties properties) {
		this.properties = properties;
	}

	/**
	 * Simple helper method to send a message to the etl discrete groundwater rdb
	 * SNS topic.
	 *
	 * @param mess The message to place in the SNS topic.
	 */
	public void publishSNSMessage(String mess) {
		PublishRequest request = new PublishRequest(mess, snsTopic.getTopicArn());

		try {
			snsClient.publish(request);
		} catch (Exception e) {
			throw new RuntimeException("Error publishing SNS topic message: " + e.getMessage(), e);
		}
	}

	private Topic getSNSTopic() {
		String topicName = String.format("%s-%s-topic", TOPIC_BASE_NAME, properties.getTier());
		Topic snsTopic = null;

		try {
			ListTopicsRequest request = new ListTopicsRequest();

			ListTopicsResult result = snsClient.listTopics(request);
			for (Topic topic : result.getTopics()) {
				String arn = topic.getTopicArn();
				if (arn != null && arn.contains(topicName)) {
					snsTopic = topic;
					break;
				}
			}

			if (snsTopic == null) {
				throw new RuntimeException("SNS topic not found: " + topicName);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error getting SNS topic: " + e.getMessage(), e);
		}

		return snsTopic;
	}

}
