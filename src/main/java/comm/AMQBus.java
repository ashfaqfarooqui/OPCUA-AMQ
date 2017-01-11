/**
 * 
 */
package comm;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONObject;
import org.json.JSONString;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * @author ashfaqf
 *
 */
public class AMQBus{

	private MessageProducer producer;
	private Session session;
	private MessageConsumer consumer;
	private Connection connection;
	private Destination destination;
	/**
	 * @param args
	 */
	
	public AMQBus (){
	}

	public void setupBus(MessageListener msgLis) {
		try {
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
					ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
			connection = connectionFactory.createConnection();

			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			destination = session.createTopic("OPCUAMilo");
			//Setup producer
			producer = session.createProducer(destination);
			
			//setup consumer
			consumer = session.createConsumer(destination);
			connection.start();
			consumer.setMessageListener(msgLis);
			
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void closeBus(){
		try {
			producer.close();
			session.close();
			connection.close();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void sendMessage(JSONObject msg){
		try{
		producer.send(session.createTextMessage(msg.toString()));
		}catch (Exception e) {
			e.printStackTrace();
		}
	}



	

}
