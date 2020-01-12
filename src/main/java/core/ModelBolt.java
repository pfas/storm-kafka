package core;

import msg.ControlMsg;
import msg.ModelMsg;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class ModelBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ModelBolt.class);

    private Map<String, Object> map;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }

    public ControlMsg callModel(Object features) {
        ControlMsg controlMsg = new ControlMsg();
        /**
         * TODO : 调用模型， 并将模型结果放到 ControlMsg 里
         *
         *  Map<Object, Object> params = new HashMap<>();
         *  try {
         *      ResponseBody responseBody = AppUtil.doGet(AppConfig.ModelServerConfig.modelUrl, new HashMap<>(), map);
         *      // TODO: 解析 responsebody to ControlMsg
         *  } catch (IOException e) {
         *      logger.error(e.getMessage());
         *      e.printStackTrace();
         *  }
         *
         */


        return controlMsg;
    }

    /**
     * 从 FeatureComputeBolt 获取特征计算结果，调用模型预测，将控制信息传递给下游
     *
     * @param tuple
     */
    public void execute(Tuple tuple) {
        ModelMsg modelMsg = (ModelMsg) tuple.getValue(0);
        ControlMsg controlMsg = callModel(modelMsg);
        outputCollector.emit(new Values(controlMsg));
        outputCollector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("model"));
    }
}
