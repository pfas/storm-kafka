package core;

import msg.ControlMsg;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
        /**
         * TODO: 调用反向控制 API
         *
         * try {
         *      AppUtil.doGet(AppConfig.ControlServerConfig.controlUrl, new HashMap<>(), new HashMap<>());
         * } catch (IOException e) {
         *      logger.error(e.getMessage());
         *      e.printStackTrace();
         * }
         *
         */

        return null;

    }


    /**
     * 从 ModelBolt 或取控制信息，调用反向控制 api
     * @param tuple
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
