package comm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.config.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.client.UaTcpStackClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpcComm {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private OpcUaClient client;

	private AtomicLong clientHandles = new AtomicLong(1L);
	public Map<String, Node> activeNodes = new HashMap<>();
	public Map<NodeId, String> nodeIdToString = new HashMap<>();
	public Map<NodeId, Object> nodeIdToCurrentValues = new HashMap<>();
	public Map<String, Object> nameToCurrentValue = new HashMap<>();

	public OpcComm() {
		logger.info("Setting up communication");
	}

	public OpcUaClient connect() throws Exception {
		return connect("opc.tcp://192.168.0.10:4840");
	}

	public OpcUaClient getClient(){
		return client;
	}
	public OpcUaClient connect(String url) throws Exception {
		if (client != null) {
			return client;
		} else {
			OpcUaClientConfigBuilder configBuilder = new OpcUaClientConfigBuilder();
			EndpointDescription[] endpoints = UaTcpStackClient.getEndpoints(url).get();
			EndpointDescription endpoint = Arrays.stream(endpoints).filter(e -> e.getEndpointUrl().equals(url))
					.findFirst().orElseThrow(() -> new Exception("no desired endpoints returned"));
			configBuilder.setEndpoint(endpoint);
			configBuilder.setApplicationName(LocalizedText.english("Codesys Lab opc-ua client"));
			configBuilder.build();
			logger.info("Config builder: {}", configBuilder.toString());
			client = new OpcUaClient(configBuilder.build());
			client.connect().get();
			return client;

		}
	}

	public void populateNodes(String indent, OpcUaClient client, NodeId browseRoot) {
		try {
			List<Node> nodes = client.getAddressSpace().browse(browseRoot).get();
			for (Node node : nodes) {
				logger.info("{} Node={}", indent, node.getBrowseName().get().getName());
				String nodeName = node.getBrowseName().get().getName();
				NodeId nodeId = node.getNodeId().get();
				activeNodes.put(nodeName, node);
				nodeIdToString.put(nodeId, nodeName);
				// recursively browse to children
				populateNodes(indent + "  ", client, nodeId);

			}
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Browsing nodeId={} failed: {}", browseRoot, e.getMessage(), e);
		}
	}

	public void subscribeToNodes(List<String> nodes) throws ClientException {
		// create a subscription @ 1000ms
		UaSubscription subscription = null;
		List<MonitoredItemCreateRequest> request = new ArrayList<MonitoredItemCreateRequest>();
		try {
			subscription = client.getSubscriptionManager().createSubscription(100).get();
			for (String node : nodes) {
				NodeId nodeId = null;
				if (activeNodes.containsKey(node)) {
					nodeId = activeNodes.get(node).getNodeId().get();
				} else {
					System.out.println("Node " + node + " does not exist in active nodes");
				}
				ReadValueId readValueId = new ReadValueId(nodeId, AttributeId.Value.uid(), null,
						QualifiedName.NULL_VALUE);
				MonitoringParameters parameters = new MonitoringParameters(uint(clientHandles.getAndIncrement()), 100.0, // sampling
						// interval
						null, // filter, null means use default
						uint(10), // queue size
						true // discard oldest
				);
				request.add(new MonitoredItemCreateRequest(readValueId, MonitoringMode.Reporting, parameters));

			}

			BiConsumer<UaMonitoredItem, Integer> onItemCreated = (item, id) -> item
					.setValueConsumer(this::onSubscriptionValue);

			List<UaMonitoredItem> items = subscription
					.createMonitoredItems(TimestampsToReturn.Both, request, onItemCreated).get();
			for (UaMonitoredItem item : items) {
				if (item.getStatusCode().isGood()) {

					System.out.println("item created for nodeId " + item.getReadValueId().getNodeId());
				} else {
					System.out.println("failed to create item for nodeId " + item.getReadValueId().getNodeId()
							+ " status" + item.getStatusCode());
				}
			}
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ClientException(e.getMessage(),e.getCause());
			
			
		}
	}

	public void onSubscriptionValue(UaMonitoredItem item, DataValue value) {
		logger.info("Main:  subscription value received: item={}, value={}", item.getReadValueId().getNodeId().toString(),
				value.getValue().toString());
		
		//nodeIdToCurrentValues.put(item.getReadValueId().getNodeId(), value.getValue());
		//nameToCurrentValue.put(nodeIdToString.get(item.getReadValueId().getNodeId()), value.getValue());

	}

	public Boolean writeValue(String node, Object value) throws ClientException {
		NodeId nodeId;
		try {
			nodeId = activeNodes.get(node).getNodeId().get();
			DataValue dv = new DataValue(new Variant(value));
			logger.info("Value to write: item={}, value={}", nodeId.toString(), dv.getValue().toString());
			return client.writeValue(nodeId, dv).get().isGood();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ClientException(e.getMessage(),e.getCause());

		}
		
	}
	


}
