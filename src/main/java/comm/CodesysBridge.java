package comm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import com.sun.org.apache.bcel.internal.classfile.Code;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodesysBridge extends OpcComm implements MessageListener {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public final List<String> monitoringNodes = Arrays.asList("ixArmRun", "ixGripping", "ixArmUp", "ixArmYPos");

	CodesysBridge bridgeObj = null;
	AMQBus amq;
	boolean sendChangesToAmq = false;

	public CodesysBridge() {
	}

	public static void main(String[] args) throws Exception {

		// final ActorSystem system = ActorSystem.create("Runner");
		// final ActorRef eventHandler =
		// system.actorOf(Props.create(EventHandler.class), "eventHandler");
		final String url = "opc.tcp://192.168.0.10:4840";
		Scanner keyboard = new Scanner(System.in);

		//amq = new AMQBus();
		//amq.setupBus(this);
		CodesysBridge bridge = new CodesysBridge();
		bridge.connect();
		bridge.populateNodes("",bridge.getClient(),Identifiers.RootFolder);
		System.out.println("Press enter twice to exit...");
		keyboard.nextLine();
        System.out.println(bridge.getClient().readValue(0.0, TimestampsToReturn.Both,bridge.activeNodes.get("OP1.initial" +
                "").getNodeId().get()).get().getValue());

		keyboard.nextLine();
		keyboard.close();
		//amq.closeBus();
		// eventHandler.tell(msg, sender);
	}

	public void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
		logger.info("subscription value received: item={}, value={}", item.getReadValueId().getNodeId().toString(),
				value.getValue().toString());
		
		nodeIdToCurrentValues.put(item.getReadValueId().getNodeId(), value.getValue());
		nameToCurrentValue.put(nodeIdToString.get(item.getReadValueId().getNodeId()), value.getValue());

	}
	@Override
	public void onMessage(Message message) {

		/**
		 * 
		 * {
		 * 		"command": "connect"
		 *      "url":   "...."
		 *      
		 *      "command":"subscribe"
		 *      "nodes":[   "node1"
		 *      			"node2"
		 *              ]
		 *              
		 *      "command":"unsubscribe"
		 *      
		 *      
		 *      "command":"writeToPlc"
		 *      "NodeValues": {
		 *       				"node1": "value1"
		 *       				"node2": "value2"
		 *                    }
		 * }
		 * 
		 * 
		 * 
		 * 
		 * 
		 */
		try {
			if (message instanceof TextMessage) {
				JSONObject jobj = new JSONObject(((TextMessage) message).getText());
				if (jobj.has("command")) {
					JSONObject resp = new JSONObject();
					switch (jobj.getString("command")) {

					case "connect":
						String url = jobj.getString("url");
						bridgeObj = new CodesysBridge();
						bridgeObj.connect(url);
						bridgeObj.populateNodes(" ", bridgeObj.getClient(), Identifiers.RootFolder);
						resp.put("response","connect");
						resp.put("activenodes",activeNodes);
						amq.sendMessage(resp);
						break;
					case "subscribe":
						if(bridgeObj != null) throw new ClientException("Bridge is not connected");
						
						bridgeObj.subscribeToNodes((jobj.getJSONArray("nodes").toList().stream()
								   .map(o -> o.toString())
								   .collect(Collectors.toList())));

						break;
					case "unsubscribe":
						if(bridgeObj != null) throw new ClientException("Bridge is not connected");

						bridgeObj.getClient().getSubscriptionManager().clearSubscriptions();
						break;
					case "writeToPlc":
						if(bridgeObj != null) throw new ClientException("Bridge is not connected");

						Map<String,Object> status = new HashMap<String,Object>();
						Map<String,Object> nodeMap = jobj.getJSONObject("NodeValues").toMap();
						for (String node : nodeMap.keySet()) {
							status.put(node,bridgeObj.writeValue(node, nodeMap.get(node)));
						}
						resp.put("response","writeToPlc");
						resp.put("status",status);
						amq.sendMessage(resp);
						
						break;
					case "request":
						/**
						 * Can request active nodes, or values for specific
						 * nodes or all the available nodes
						 */
						resp.put("response","request");
						resp.put("activeNodes", activeNodes);
						resp.put("subscribedValues", nameToCurrentValue);
						amq.sendMessage(resp);
						break;
					case "disconnect":
						bridgeObj.getClient().disconnect().get();
						bridgeObj = null;
						break;
						
					case "listenToChanges":
						sendChangesToAmq = jobj.getBoolean("value");
						
						break;

					}
				}

			}
		} catch (Exception e) {
			JSONObject jobj = new JSONObject();
			jobj.put("response", "Exception");
			jobj.put("message", e.getMessage());
			jobj.put("cause", e.getCause());
			
			amq.sendMessage(jobj);
			// Handle the exception appropriately
		}

	}

}
