package core;


import msg.ModelMsg;
import msg.ProcessMsg;
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
import java.util.Queue;

public class FeatureComputeBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(FeatureComputeBolt.class);

    private Map<String, Object> map;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;

    public FeatureComputeBolt() {

    }

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }


    private ModelMsg computeFeature(int windowSize, int blockSize, Queue<Object> window) {
        ModelMsg modelMsg = new ModelMsg();
        //TODO: 特征计算, 并把特征计算结果放在 ModelMsg 中
        return  modelMsg;
    }


    /**
     * 从 WindowBolt 处接收窗口并计算特征，将计算结果传递给下游
     * @param tuple
     */
    public void execute(Tuple tuple) {
        ProcessMsg processMsg = (ProcessMsg) tuple.getValue(0);
        ModelMsg modelMsg = computeFeature(processMsg.getWindowSize(), processMsg.getBlockSize(), processMsg.getWindow());
        outputCollector.emit(new Values(modelMsg));
        outputCollector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("features"));
    }
}
