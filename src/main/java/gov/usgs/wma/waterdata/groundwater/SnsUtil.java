package gov.usgs.wma.waterdata.groundwater;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

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
	private final Topic snsTopic;

	private Properties properties;

	@Autowired
	SnsUtil(Properties properties) {
		this.properties = properties;
		this.snsTopic = getSNSTopic();
	}

	/**
	 * Simple helper method to send a message to the etl discrete groundwater rdb
	 * SNS topic. Exceptions are caught and logged to standard error, so that the rdb
	 * processing continues.
	 *
	 * @param mess The message to place in the SNS topic.
	 */
	public void publishSNSMessage(String mess) {
		if (snsTopic != null) {
			try {
				PublishRequest request = new PublishRequest(mess, snsTopic.getTopicArn());
				snsClient.publish(request);
				System.out.println("INFO: Message published to SNS: " + mess);
			} catch (Exception e) {
				System.err.print("Error publishing message to SNS topic: " + e.getMessage());
				System.err.print("Message to have been sent: " + mess);
				e.printStackTrace();
			}
		} else {
			System.err.print("Error SNS logging not initialized, message to have been sent: " + mess);
		}
	}

	private Topic getSNSTopic() {
		Topic snsTopic = null;
		String tier = properties == null ? null : properties.getTier();
		String mess = "";

		if (StringUtils.hasText(tier)) {
			try {
				String topicName = String.format("%s-%s-topic", TOPIC_BASE_NAME, tier);
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
					System.err.println("Error initializing SNS logging: SNS topic not found: " + topicName);
				}
			} catch (Exception e) {
				System.err.println("Error getting SNS topic: " + e.getMessage());
				e.printStackTrace();
			}
		} else {
			// Todo: use logging framework
			mess = properties == null ? "properties component not available" : "tier property not set";
			System.err.print("Error initializing SNS logging: " + mess);
		}

		return snsTopic;
	}

}
