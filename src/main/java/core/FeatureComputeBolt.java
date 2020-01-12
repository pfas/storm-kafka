package core;


import msg.IoTMsg;
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
import util.FeatureUtil;

import java.util.*;

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


    private ModelMsg computeFeature(Collection<IoTMsg> window, int windowSize, int blockSize) {
        ModelMsg modelMsg = new ModelMsg();
        List<List<IoTMsg>> splits = split(new ArrayList<>(window), windowSize, blockSize);
        List<List<IoTMsg>> features = calculate(splits);
        merge(modelMsg, features);
        return modelMsg;
    }


    /**
     * 构造为模型需要的特征
     */
    private void merge(ModelMsg modelMsg, List<List<IoTMsg>> features) {
        List<Float> meanList = new ArrayList<>();
        List<Float> stdList = new ArrayList<>();
        List<Float> integralList = new ArrayList<>();
        List<Float> skewList = new ArrayList<>();
        List<Float> kurtosisList = new ArrayList<>();
        for (List<IoTMsg> feature : features) {
            IoTMsg mean = feature.get(0);
            IoTMsg std = feature.get(1);
            IoTMsg integral = feature.get(2);
            IoTMsg skew = feature.get(3);
            IoTMsg kurtosis = feature.get(4);

            meanList.addAll(Arrays.asList(mean.generate()));
            stdList.addAll(Arrays.asList(std.generate()));
            integralList.addAll(Arrays.asList(integral.generate()));
            skewList.addAll(Arrays.asList(skew.generate()));
            kurtosisList.addAll(Arrays.asList(kurtosis.generate()));
        }
        modelMsg.setMean(meanList);
        modelMsg.setStd(stdList);
        modelMsg.setIntegral(integralList);
        modelMsg.setSkew(skewList);
        modelMsg.setKurtosis(kurtosisList);
    }

    /**
     * 对于每一个split进行特征计算
     */
    private List<List<IoTMsg>> calculate(List<List<IoTMsg>> splits) {
        List<List<IoTMsg>> features = new ArrayList<>();
        for (List<IoTMsg> oneSplit : splits) {
            List<IoTMsg> featurePerSplit = new ArrayList<>();
            IoTMsg[] oneSplitArray = oneSplit.toArray(new IoTMsg[0]);

            IoTMsg mean = FeatureUtil.calcMean(oneSplitArray);
            featurePerSplit.add(mean);
            featurePerSplit.add(FeatureUtil.calcStd(oneSplitArray, mean));
            featurePerSplit.add(FeatureUtil.calcIntegral(oneSplitArray));
            featurePerSplit.add(FeatureUtil.calcSkew(oneSplitArray, mean));
            featurePerSplit.add(FeatureUtil.calcKurtosis(oneSplitArray, mean));

            features.add(featurePerSplit);
        }
        return features;
    }

    /**
     * 将 windowSize 长度的数据进行划分
     */
    private List<List<IoTMsg>> split(List<IoTMsg> window, int windowSize, int blockSize) {
        List<List<IoTMsg>> splits = new ArrayList<>();
        if (windowSize == 0) {
            return splits;
        }

        int splitsNum = windowSize / blockSize;
        for (int i = 0; i < splitsNum; i++) {
            splits.add(window.subList(blockSize * i, blockSize * (i + 1)));
        }
        return splits;
    }


    /**
     * 从 WindowBolt 处接收窗口并计算特征，将计算结果传递给下游
     *
     * @param tuple
     */
    public void execute(Tuple tuple) {
        ProcessMsg processMsg = (ProcessMsg) tuple.getValue(0);
        ModelMsg modelMsg = computeFeature(processMsg.getWindow(), processMsg.getWindowSize(), processMsg.getBlockSize());
        outputCollector.emit(new Values(modelMsg));
        outputCollector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("features"));
    }
}
