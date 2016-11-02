package hortonworks.hdp.refapp.trucking.storm.topology;

import hortonworks.hdp.refapp.trucking.storm.bolt.alert.TruckEventRuleBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.alert.window.SlidingWindowAverageSpeedBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.alert.window.TumblingWindowInfractionCountV2Bolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.alert.window.rule.InfractionRulesV2Bolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.hbase.TruckHBaseBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.hdfs.FileTimeRotationPolicy;
import hortonworks.hdp.refapp.trucking.storm.bolt.hive.HiveTablePartitionHiveServer2Action;
import hortonworks.hdp.refapp.trucking.storm.bolt.join.JoinStreamsBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.join.JoinTruckSpeedAndDriverInfractionBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.phoenix.TruckPhoenixHBaseBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.solr.SolrIndexingBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.websocket.WebSocketBolt;
import hortonworks.hdp.refapp.trucking.storm.kafka.TruckEventSchema;
import hortonworks.hdp.refapp.trucking.storm.kafka.TruckSpeedEventSchema;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TruckEventProcessorKafkaTopologyPhase3 extends BaseTruckEventTopology {
	
	private static final Logger LOG = LoggerFactory.getLogger(TruckEventProcessorKafkaTopologyPhase3.class);

	// HBase table names and column families
	private static final String DANGEROUS_EVENTS_TABLE_NAME = "driver_dangerous_events";
	private static final String EVENTS_TABLE_COLUMN_FAMILY_NAME = "events";

	private static final String EVENTS_TABLE_NAME = "driver_events";
	private static final String ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME = "allevents";

	private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events_count";
	private static final String EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME = "counters";

	// HBase RowKey
	private static final String HBASE_ROW_KEY = "eventKey";

	// Spout and bolt names
	private static final String HBASE_BOLT_DANGEROUS_EVENTS = "hbaseDangerousEvents";
	private static final String HBASE_BOLT_DRIVER_INCIDENT_COUNT = "hbaseDangerousEventsCount";
	private static final String HBASE_BOLT_ALL_EVENTS = "hbaseAllDriverEvents";
	
	public TruckEventProcessorKafkaTopologyPhase3(String configFileLocation) throws Exception {
		super(configFileLocation);			
	}
	
	public TruckEventProcessorKafkaTopologyPhase3(Properties prop) throws Exception {
		this.topologyConfig = prop;	
	}
	
	public StormTopology buildTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		
		/* Set up Kafka Spout to ingest truck events */
		configureTruckEventsKafkaSpout(builder);
		
		/* Set up Kafka Spout to ingest truck speed events */
		configureTruckSpeedEventsKafkaSpout(builder);
		
		/* Join the Truck Event and Speed Streams Together */
		configureJoinTruckStreamsBolt(builder);
		
		/* Configuring Windowing to count infractions every 3 minutes */
		//configureTumblingWindowInfractionCountBolt(builder);
		
		/* Sliding window to calculate speed */
		//configureSlidingWindowTruckAverageSpeed(builder);
		
		/* Join Average and InfractionsCount */
		//configureJoinAverageSpeedAndInfractionCount(builder);
		
		/* Configure Pattern Matching rule for number of infractions */
		//configureInfractionRulesBolt(builder);
		
		/* Setup HBase bolts to persist violations and all events*/
		configureHBaseBoltAllEvents(builder);
		configureHBaseBoltDangerousEvents(builder);
		configureHBaseBoltDangerousEventsCount(builder);
		
		/* Setup WebSocket Bolt for alerts and notifications Dashboard */
		//configureWebSocketBolt(builder);

		/* Set up KafkaStore Bolt to pushing values out to a Kafka topic */
		configureKafkaStoreBolt(builder);
		
		return builder.createTopology();
	}

	/* Spout and Bolt setup */
	private int configureTruckEventsKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = new KafkaSpout(truckEventKafkaSpoutConfig());

		int spoutCount = Integer.valueOf(topologyConfig.getProperty("trucking.spout.thread.count"));
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));

		builder.setSpout("Truck-Events-Stream", kafkaSpout, spoutCount);
		return boltCount;
	}

	private int configureTruckSpeedEventsKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = new KafkaSpout(truckSpeedEventKafkaSpoutConfig());

		int spoutCount = Integer.valueOf(topologyConfig.getProperty("trucking.spout.thread.count"));
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));

		builder.setSpout("Truck-Speed-Events-Stream", kafkaSpout, spoutCount);
		return boltCount;
	}

	private void configureJoinTruckStreamsBolt(TopologyBuilder builder) {
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		Duration windowLength = new Duration(5, TimeUnit.SECONDS);
		builder.setBolt("Join-Truck-Streams",
				new JoinStreamsBolt().withTumblingWindow(windowLength), boltCount)
				.fieldsGrouping("Truck-Events-Stream", new Fields("driverId"))
				.fieldsGrouping("Truck-Speed-Events-Stream", new Fields("driverId"));
	}

	private void configureTumblingWindowInfractionCountBolt(TopologyBuilder builder) {
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		//Duration windowLength = new Duration(2, TimeUnit.MINUTES);
		Duration windowLength = new Duration(20, TimeUnit.SECONDS);
		builder.setBolt("3-Min-Count-Window",
				new TumblingWindowInfractionCountV2Bolt().withTumblingWindow(windowLength), boltCount).
				fieldsGrouping("Join-Truck-Streams", new Fields("driverId"));
	}

	private void configureSlidingWindowTruckAverageSpeed(TopologyBuilder builder) {
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		Duration windowLength = new Duration(1, TimeUnit.MINUTES);
		Duration timeInterval = new Duration(3, TimeUnit.MINUTES);
		builder.setBolt("5-Min-Sliding-Avg-Speed",
				new SlidingWindowAverageSpeedBolt().withWindow(windowLength, timeInterval), boltCount).
				fieldsGrouping("Join-Truck-Streams", new Fields("driverId"));
	}

	private void configureJoinAverageSpeedAndInfractionCount(TopologyBuilder builder) {
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		//Duration windowLength = new Duration(2, TimeUnit.MINUTES);
		Duration windowLength = new Duration(30, TimeUnit.SECONDS);
		builder.setBolt("Join-AvgSpeed-And-InfractionCount",
				new JoinTruckSpeedAndDriverInfractionBolt().withTumblingWindow(windowLength), boltCount)
				.fieldsGrouping("5-Min-Sliding-Avg-Speed", new Fields("driverId"))
				.fieldsGrouping("3-Min-Count-Window", new Fields("driverId"));
	}

	private void configureInfractionRulesBolt(TopologyBuilder builder) {
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		builder.setBolt("Infractions-Rules-Engine",
				new InfractionRulesV2Bolt(topologyConfig), boltCount)
				.shuffleGrouping("Join-AvgSpeed-And-InfractionCount");
	}

	/* 3 HBase Bolts */
	private void configureHBaseBoltAllEvents(TopologyBuilder builder) {
		HBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField(HBASE_ROW_KEY)
				.withColumnFields(getAllFields())
				.withColumnFamily(ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME);

		HBaseBolt hbaseDriverEventsTable = new HBaseBolt(EVENTS_TABLE_NAME, mapper)
				.withConfigKey("hbase.conf");

		builder.setBolt(HBASE_BOLT_ALL_EVENTS, hbaseDriverEventsTable, 1)
				//.fieldsGrouping("Join-Truck-Streams", getAllFields());
				.shuffleGrouping("Join-Truck-Streams"); // For testing
	}

	private void configureHBaseBoltDangerousEvents(TopologyBuilder builder) {
		HBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField(HBASE_ROW_KEY)
				.withColumnFields(getAllFields())
				.withColumnFamily(EVENTS_TABLE_COLUMN_FAMILY_NAME);

		HBaseBolt hbaseDriverDangerousEventsTable =
				new HBaseBolt(DANGEROUS_EVENTS_TABLE_NAME, mapper).withConfigKey("hbase.conf");

		// TODO: why filtered on getAllFields() when others just on driverID?  Both should let the same tuples in?
		builder.setBolt(HBASE_BOLT_DANGEROUS_EVENTS, hbaseDriverDangerousEventsTable, 1)
				.fieldsGrouping("Join-Truck-Streams", "Non-Normal-Events", getAllFields());
				//.fieldsGrouping("Join-Truck-Streams", new Fields("driverId")); // For testing
	}

	private void configureHBaseBoltDangerousEventsCount(TopologyBuilder builder) {
		HBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField("driverId")
				.withColumnFields(new Fields("driverId"))
				//.withCounterFields(new Fields("incidentTotalCount"))
				// TODO: reimplement counter as a field
				.withColumnFamily(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME);

		HBaseBolt hbaseEventsCountTable =
				new HBaseBolt(EVENTS_COUNT_TABLE_NAME, mapper).withConfigKey("hbase.conf");

		builder.setBolt(HBASE_BOLT_DRIVER_INCIDENT_COUNT, hbaseEventsCountTable, 1)
				.fieldsGrouping("Join-Truck-Streams", new Fields("driverId"));
	}

	/*
	private void configureWebSocketBolt(TopologyBuilder builder) {
		boolean configureWebSocketBolt = Boolean.valueOf(topologyConfig.getProperty("trucking.notification.topic")).booleanValue();
		if(configureWebSocketBolt) {
			WebSocketBolt webSocketBolt = new WebSocketBolt(topologyConfig);
			builder.setBolt("Notification-Dashboard", webSocketBolt, 4).shuffleGrouping("Join-Truck-Streams");
		}
	}*/

	private void configureKafkaStoreBolt(TopologyBuilder builder) {
		// TODO: Specify Kafka producer properties if necessary - http://storm.apache.org/releases/1.0.2/storm-kafka.html
		// Producer properties for KafkaBolt
		Properties props = new Properties();
		//props.put("metadata.broker.list", "localhost:9092"); // deprecated
		//props.put("acks", "1");
		//props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("bootstrap.servers", "sandbox.hortonworks.com:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

		KafkaBolt kafkaStoreBolt = new KafkaBolt()
				.withTopicSelector(new DefaultTopicSelector("test_topics"))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())
				.withProducerProperties(props);

		builder.setBolt("Kafka-Store", kafkaStoreBolt, 1).shuffleGrouping("Join-Truck-Streams");
	}




	/**
	 * Construct the KafkaSpout which comes from the jar storm-kafka-0.8-plus
	 * @return
	 */
	private SpoutConfig truckEventKafkaSpoutConfig() {
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host"));
		String topic = topologyConfig.getProperty("trucking.kafka.topic");
		//String zkRoot = topologyConfig.getProperty("kafka.zookeeper.znode.parent");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = topologyConfig.getProperty("trucking.kafka.consumer.group.id");

		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

		/* Custom TruckScheme that will take Kafka message of single truckEvent
		 * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
		 * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
		spoutConfig.scheme = new SchemeAsMultiScheme(new TruckEventSchema());

		return spoutConfig;
	}
	
	private SpoutConfig truckSpeedEventKafkaSpoutConfig() {
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host"));
		String topic = topologyConfig.getProperty("trucking.speed.kafka.topic");
		//String zkRoot = topologyConfig.getProperty("kafka.zookeeper.znode.parent");
		String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
		String consumerGroupId = topologyConfig.getProperty("trucking.speed.kafka.consumer.group.id");

		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);

		/* Custom TruckScheme that will take Kafka message of single truckEvent
		 * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
		 * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
		spoutConfig.scheme = new SchemeAsMultiScheme(new TruckSpeedEventSchema());

		return spoutConfig;
	}

	private Fields getAllFields() {
		//return new Fields("driverId", "truckId", "ts", "eventType", "latitude", "longitude", "driverName", "routeId", "routeName", "hbaseRowKey");
		return new Fields("driverId", "truckId", "eventTime", "eventType", "truckSpeed", "longitude", "latitude", "eventKey", "correlationId", "driverName", "routeId", "routeName");
	}




	public static void main(String[] args) throws Exception {
		String configFileLocation = args[0];
		TruckEventProcessorKafkaTopologyPhase3 truckTopology = new TruckEventProcessorKafkaTopologyPhase3(configFileLocation);
		truckTopology.buildAndSubmit();
	}

	private void buildAndSubmit() throws Exception {
		StormTopology topology = buildTopology();

		/* This conf is for Storm and it needs be configured with things like the following:
		 * 	Zookeeper server, nimbus server, ports, etc... All of this configuration will be picked up
		 * in the ~/.storm/storm.yaml file that will be located on each storm node.
		 */
		Config conf = new Config();
		conf.setDebug(true);
		conf.setMessageTimeoutSecs(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.message.timeout.secs")));
		//conf.put("kafka.broker.properties.bootstrap.servers", topologyConfig.getProperty("kafka.broker.properties.bootstrap.servers"));
		//conf.put("bootstrap.servers", "sandbox.hortonworks.com:2181");
		// Disabled above 2 lines ... otherwise get
		//.Client [INFO] creating Netty Client, connecting to sandbox.hortonworks.com/<IP>:<port> errrors


		// Temporary - adding from jRepo
		//1Map<String, Object> hbaseConf = new HashMap<String, Object>();
		// From: http://stackoverflow.com/questions/29664742/storm-hbase-configuration-not-found
		Map<String, String> someMap = new HashMap<String, String>();
		//someMap.put("hbase.rootdir2", "/hbase-unsecure");
		Properties hbaseConf = new Properties();
		//hbaseConf.put("hbase.rootdir","hdfs://<IP Address>:8020/hbase");
		//hbaseConf.put("hbase.zookeeper.znode.parent","hbase-unsecure");
		//hbaseConf.put("hbase.rootdir", "\\\\/hbase-unsecure"); // I think it's this one.
		//hbaseConf.put("hbase.rootdir", "/hbase-unsecure"); // I think it's this one.
		//hbaseConf.put("hbase.rootdir1", "\\/hbase-unsecure"); // I think it's this one.
		//hbaseConf.put("hbase.rootdir2", "\\\\/hbase-unsecure"); // I think it's this one.
		//hbaseConf.put("hbase.rootdir3", "\\\\\\/hbase-unsecure"); // I think it's this one.
		//hbaseConf.put("hbase.rootdir4", "\\\\\\\\/hbase-unsecure"); // I think it's this one.
		//hbaseConf.put("hbase.rootdir5", "/hbase-unsecure".replaceAll("/", Matcher.quoteReplacement("\\/"))); // I think it's this one.
		//hbaseConf.put("hbase.rootdir6", "Fhbase-unsecure".replaceAll("F", Matcher.quoteReplacement("\\/"))); // I think it's this one.
		//hbaseConf.put("hbase.rootdir", topologyConfig.getProperty("hbase.zookeeper.znode.parent"));
		//conf.put("hbase.conf", hbaseConf);
		//conf.put("hbase.conf2", someMap);
		//conf.put("hbase.conf3", "/hbase-unsecure");
		//conf.put("hbase.conf4", "\\\\/hbase-unsecure");

		// From: https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.5.0/bk_storm-component-guide/content/storm-hbase-kerb.html
		//conf.put(HBaseSecurityUtil.STORM_KEYTAB_FILE_KEY, "$keytab");
		//conf.put(HBaseSecurityUtil.STORM_USER_NAME_KEY, "$principal");

		/* Set the number of workers that will be spun up for this topology.
		 * Each worker represents a JVM where executor thread will be spawned from */
		Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("trucking.storm.trucker.topology.workers"));
		conf.setNumWorkers(topologyWorkers);
		//conf.put("zookeeper.znode.parent", "sandbox.hortonworks.com:2181/hbase-unsecure");
		//conf.put("hbase.zookeeper.znode.parent", "sandbox.hortonworks.com:2181/hbase-unsecure");
		//conf.put("hbase.rootdir", "sandbox.hortonworks.com:2181/hbase-unsecure");
		//conf.put("hbase.conf2", "sandbox.hortonworks.com:2181/hbase-unsecure");

		Map<String, String> HBConfig = new HashMap<String, String>();
		//HBConfig.put("hbase.rootdir22","hdfs://sandbox.hortonworks.com:8121/hbase-unsecure");

		String zookeeperHost = topologyConfig.getProperty("hbase.zookeeper.host");
		String zookeeperClientPort = topologyConfig.getProperty("hbase.zookeeper.client.port");
		String zookeeperZNodeParent = topologyConfig.getProperty("hbase.zookeeper.znode.parent");

		//HBConfig.put("hbase.zookeeper.quorum", zookeeperHost);
		//HBConfig.put("hbase.zookeeper.property.clientPort", zookeeperClientPort);
		HBConfig.put("zookeeper.znode.parent", zookeeperZNodeParent);
		conf.put("hbase.conf", HBConfig);

		Configuration config = HBaseConfiguration.create();
		//config.set("hbase.zookeeper.quorum", zookeeperHost);
		config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
		config.set("zookeeper.znode.parent", zookeeperZNodeParent);
		//conf.put("hbase.conf", config);



		//conf.put("hbase.zookeeper.quorum", zookeeperHost);
		//conf.put("hbase.zookeeper.property.clientPort", zookeeperClientPort);
		//conf.put("zookeeper.znode.parent", zookeeperZNodeParent);


		//conf.put("hbase.conf", HBConfig);

		// Producer properties for KafkaBolt
		Properties props = new Properties();
		//props.put("metadata.broker.list", "localhost:9092"); // deprecated
		//props.put("acks", "1");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("bootstrap.servers", "sandbox.hortonworks.com:2181");
		//conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);




		conf.put("bootstrap.servers", "sandbox.hortonworks.com:2181");
		conf.put("zookeeper.znode.parent", topologyConfig.getProperty("zookeeper.znode.parent"));

		try {
			StormSubmitter.submitTopology("truck-event-processor", conf, topology);
		} catch (Exception e) {
			LOG.error("Error submitting Topology", e);
		}
	}
}
