package core;


import config.AppConfig;
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

        ModelMsg modelMsg;
        if ("test".equals(AppConfig.AppGlobalConfig.env)) {
            modelMsg = computeFeature(getMockData(), processMsg.getWindowSize(), processMsg.getBlockSize());
        } else {
            modelMsg = computeFeature(processMsg.getWindow(), processMsg.getWindowSize(), processMsg.getBlockSize());
        }

        modelMsg.setStage(processMsg.getStage());
        modelMsg.setBrand(processMsg.getBrand());
        modelMsg.setIndex(processMsg.getIndex());
        modelMsg.setBatch(processMsg.getBatch());
        modelMsg.setTime(processMsg.getTime());

        outputCollector.emit(new Values(modelMsg));
        outputCollector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("features"));
    }

    private static List<IoTMsg> getMockData() {
        List<IoTMsg> list = new ArrayList<>();
        list.add(new IoTMsg(new Float[]{-10.66f, 0.390567f, 19.10618f, -37.24573f, 131.4467f, 133.5563f, 118.8188f, 119.0817f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3917936f, 19.10294f, -36.03656f, 131.5355f, 133.5365f, 118.789f, 119.0544f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.39186809999999994f, 19.09534f, -36.36182f, 131.6285f, 133.517f, 118.7584f, 119.0544f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3925209f, 19.09828f, -36.14838f, 131.7247f, 133.4983f, 118.7219f, 119.028f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3935966f, 19.0913f, -34.99872f, 131.8171f, 133.4808f, 118.6879f, 119.0018f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3935158f, 19.08923f, -34.17712f, 131.9119f, 133.4808f, 118.6502f, 118.9757f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.39336220000000005f, 19.09511f, -33.95856f, 132.002f, 133.4636f, 118.6066f, 118.95f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3934222f, 19.09287f, -37.33533f, 132.0914f, 133.4468f, 118.5597f, 118.9253f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3938643f, 19.10143f, -40.27594f, 132.1814f, 133.5018f, 118.5081f, 118.9253f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3950312f, 19.11732f, -40.57544f, 132.2754f, 133.5628f, 118.4511f, 118.902f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3962857f, 19.13039f, -39.43634f, 132.4177f, 133.6252f, 118.3662f, 118.8592f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3966686f, 19.13431f, -38.67712f, 132.4684f, 133.6252f, 118.3662f, 118.8592f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3961982f, 19.13636f, -37.90149f, 132.5706f, 133.6861f, 118.3096f, 118.8592f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3959692f, 19.13407f, -38.64209f, 132.668f, 133.7445f, 118.208f, 118.8192f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3967118f, 19.131f, -39.16083f, 132.7705f, 133.7985f, 118.1487f, 118.8192f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3967991f, 19.12304f, -39.08484f, 132.8776f, 133.8463f, 118.0799f, 118.8002f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3967046f, 19.11835f, -38.4118f, 132.9787f, 133.8904f, 118.0117f, 118.7809f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3974093f, 19.10254f, -35.63397f, 133.0792f, 133.8904f, 117.9779f, 118.7617f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3975635f, 19.09483f, -35.58679f, 133.1754f, 133.9305f, 117.8748f, 118.7431f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3966887f, 19.09869f, -38.23877f, 133.2652f, 133.9676f, 117.8025f, 118.7256f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3969766f, 19.10436f, -39.44427f, 133.3629f, 134.0015f, 117.7309f, 118.7256f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3983358f, 19.09719f, -37.98047f, 133.4648f, 134.0328f, 117.6932f, 118.7085f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3983485f, 19.0858f, -36.78503f, 133.5604f, 134.0634f, 117.6129f, 118.6918f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3978139f, 19.09092f, -32.84259f, 133.6593f, 134.0634f, 117.5011f, 118.6771f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3978643f, 19.09179f, -32.65704f, 133.7545f, 134.0947f, 117.4237f, 118.6553f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3988149f, 19.07274f, -33.69733f, 133.8405f, 134.125f, 117.3438f, 118.6335f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3998042f, 19.05736f, -33.63373f, 133.9232f, 134.1529f, 117.3047f, 118.6335f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.4004687f, 19.05311f, -36.7984f, 134.0056f, 134.1763f, 117.1964f, 118.6119f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.398584f, 19.06423f, -35.14539f, 134.1211f, 134.1963f, 117.0936f, 118.5678f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.3993099f, 19.06591f, -33.87683f, 134.2014f, 134.2146f, 117.0603f, 118.5678f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.4001607f, 19.07098f, -33.3526f, 134.2736f, 134.2312f, 116.9553f, 118.5471f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.4001462f, 19.07479f, -33.9978f, 134.3081f, 134.2312f, 116.9553f, 118.5471f}));
        list.add(new IoTMsg(new Float[]{-10.67f, 0.40013059999999995f, 19.08786f, -35.16138f, 134.3788f, 134.2463f, 116.8491f, 118.5275f}));
        list.add(new IoTMsg(new Float[]{-10.67f, 0.4009608f, 19.10514f, -38.0509f, 134.473f, 134.2599f, 116.7448f, 118.4902f}));
        list.add(new IoTMsg(new Float[]{-10.67f, 0.4004962f, 19.10489f, -38.17627f, 134.5295f, 134.2738f, 116.6788f, 118.4729f}));
        list.add(new IoTMsg(new Float[]{-10.66f, 0.4005843f, 19.10673f, -35.11194f, 134.5855f, 134.2875f, 116.6197f, 118.4729f}));
        list.add(new IoTMsg(new Float[]{-10.649999999999999f, 0.40053790000000006f, 19.10951f, -33.95453f, 134.6357f, 134.2995f, 116.5629f, 118.4558f}));
        list.add(new IoTMsg(new Float[]{-10.579999999999998f, 0.4005851f, 19.11929f, -34.87567f, 134.6809f, 134.3107f, 116.5301f, 118.4737f}));
        list.add(new IoTMsg(new Float[]{-10.5f, 0.40041809999999994f, 19.13594f, -35.61298f, 134.7256f, 134.3107f, 116.4369f, 118.5312f}));
        list.add(new IoTMsg(new Float[]{-10.36f, 0.4001424f, 19.1466f, -38.61945f, 134.7633f, 134.3203f, 116.4095f, 118.5312f}));
        list.add(new IoTMsg(new Float[]{-10.229999999999999f, 0.3995191f, 19.15343f, -41.18134f, 134.799f, 134.327f, 116.3566f, 118.59299999999999f}));
        list.add(new IoTMsg(new Float[]{-9.92f, 0.3988888f, 19.15899f, -41.29034f, 134.8316f, 134.3317f, 116.3066f, 118.6552f}));
        list.add(new IoTMsg(new Float[]{-9.16f, 0.3981453f, 19.11916f, -42.44208f, 134.8687f, 134.3358f, 116.2307f, 118.7153f}));
        list.add(new IoTMsg(new Float[]{-8.889999999999999f, 0.3985295f, 19.10333f, -43.32147f, 134.881f, 134.3392f, 116.206f, 118.7722f}));
        list.add(new IoTMsg(new Float[]{-8.309999999999999f, 0.3992882f, 19.08613f, -45.37762f, 134.9033f, 134.3392f, 116.1595f, 118.7722f}));
        list.add(new IoTMsg(new Float[]{-7.819999999999999f, 0.3999003f, 19.06352f, -45.42078f, 134.9187f, 134.3428f, 116.1217f, 118.8234f}));
        list.add(new IoTMsg(new Float[]{-7.279999999999999f, 0.40041609999999994f, 19.05226f, -41.09686f, 134.9331f, 134.3476f, 116.091f, 118.8694f}));
        list.add(new IoTMsg(new Float[]{-6.799999999999999f, 0.4002319f, 19.06224f, -38.0658f, 134.9457f, 134.3537f, 116.0745f, 118.9109f}));
        list.add(new IoTMsg(new Float[]{-6.339999999999999f, 0.399815f, 19.07063f, -39.04053f, 134.9539f, 134.3601f, 116.055f, 118.9493f}));
        list.add(new IoTMsg(new Float[]{-5.909998999999999f, 0.4000592f, 19.07561f, -41.19458f, 134.9624f, 134.3657f, 116.0455f, 118.9851f}));
        return list;
    }
}
