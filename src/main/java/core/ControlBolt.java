package core;

import config.AppConfig;
import msg.ControlMsg;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.AppUtil;

import java.io.IOException;
import java.util.*;

public class ControlBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ControlBolt.class);

    private Map<String, Object> map;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }


    private Object callControl(ControlMsg controlMsg) {
        List<Map<String, Object>> params = new ArrayList<>();
        Map<String, Object> region1 = new HashMap<>();
        region1.put("Address", "5H.5H.K1_Z7TT1VALUE_1");
        region1.put("Value", controlMsg.getTempRegion1());
        region1.put("DataType", "float");

        Map<String, Object> region2 = new HashMap<>();
        region2.put("Address", "5H.5H.K1_Z7TT1VALUE_4");
        region2.put("Value", controlMsg.getTempRegion2());
        region2.put("DataType", "float");

        params.add(region1);
        params.add(region2);

        try {
            AppUtil.doPost(AppConfig.ControlServerConfig.controlUrl, String.valueOf(new JSONObject(params)));
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return null;

    }

    /**
     * 从 ModelBolt 或取控制信息，调用反向控制 api
     */
    public void execute(Tuple tuple) {
        ControlMsg controlMsg = (ControlMsg) tuple.getValue(0);
        callControl(controlMsg);
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("control"));
    }
}
