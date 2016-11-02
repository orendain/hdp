package hortonworks.hdp.refapp.trucking.storm.bolt.hbase;


        import org.apache.storm.task.OutputCollector;
        import org.apache.storm.task.TopologyContext;

        import org.apache.storm.topology.OutputFieldsDeclarer;
        import org.apache.storm.topology.base.BaseRichBolt;
        import org.apache.storm.tuple.Fields;
        import org.apache.storm.tuple.Tuple;
        import org.apache.storm.tuple.Values;
        import org.apache.log4j.Logger;

        import java.sql.Timestamp;

//new imported packages for TruckHBaseBolt

        import java.util.Map;


public class StoragePrepBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(StoragePrepBolt.class);
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple input) {
        int driverId = input.getIntegerByField("driverId");
        int truckId = input.getIntegerByField("truckId");
        long eventTime = ((Timestamp) input.getValueByField("eventTime")).getTime();
        String eventType = input.getStringByField("eventType");
        int truckSpeed = input.getIntegerByField("truckSpeed");
        double longitude = input.getDoubleByField("longitude");
        double latitude = input.getDoubleByField("latitude");
        String eventKey = input.getStringByField("eventKey");
        long correlationId = input.getLongByField("correlationId");
        String driverName = input.getStringByField("driverName");
        int routeId = input.getIntegerByField("routeId");
        String routeName = input.getStringByField("routeName");

        // All events go into the "default" stream
        outputCollector.emit(input, new Values(driverId, truckId, eventTime, eventType, truckSpeed, longitude, latitude,
                eventKey, correlationId, driverName, routeId, routeName));

        // Not Normal Events as a separate stream
        if (!eventType.equals("Normal")) {
            outputCollector.emit("Non-Normal-Events", input, new Values(driverId, truckId, eventTime, eventType, truckSpeed, longitude, latitude,
                    eventKey, correlationId, driverName, routeId, routeName));
        }

        // Acknowledge input was received
        outputCollector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("driverId", "truckId", "eventTime", "eventType", "truckSpeed", "longitude", "latitude",
                "eventKey", "correlationId", "driverName", "routeId", "routeName"));
        declarer.declareStream("Non-Normal-Events", new Fields("driverId", "truckId", "eventTime", "eventType", "truckSpeed", "longitude", "latitude",
                "eventKey", "correlationId", "driverName", "routeId", "routeName"));
    }

}
