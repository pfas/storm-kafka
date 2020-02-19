package core;

import gherkin.deps.com.google.gson.Gson;
import config.AppConfig;
import msg.IoTMsg;
import msg.OriginalMsg;
import msg.ProcessMsg;
import okhttp3.Response;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.AppUtil;

import java.io.IOException;
import java.util.*;

/**
 * 用于维护窗口大小的 Bolt
 * 从 kafka 处接收数据，并将数据维护在 window 内
 */
public class WindowsBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(BaseRichBolt.class);
    private Map<String, Object> map;
    private TopologyContext topologyContext;
    private OutputCollector outputCollector;
    private Queue<IoTMsg> window = new LinkedList<>();
    private Gson gson;

    private String currentBatch = null;
    private long startIndex = 0;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
        this.gson = new Gson();
    }

    private int[] getWindowSize(String stage) {
        try {
            Map<String, Object> query = new HashMap<>();
            query.put("stage", stage);
            Response response = AppUtil.doGet(
                    AppConfig.ModelServerConfig.modelConfigUrl,
                    new HashMap<>(),
                    query
            );
            JSONObject json = new JSONObject(Objects.requireNonNull(response.body()).string());
            return new int[]{json.getInt("window_size"), json.getInt("block_size")};
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return new int[]{50, 5};
    }


    /**
     * 解析 tuple 里面的参数信息
     * 得到 OriginalMsg
     */
    public OriginalMsg parseTuple(Tuple tuple) {
        String str = tuple.getSourceComponent();
        System.out.println("qihang: " + str);
        return gson.fromJson(str, OriginalMsg.class);
        // return null;
    }


    /**
     * 根据 index 判断 stage
     */
    public String determineStage(OriginalMsg msg) {
        if (startIndex < 200) {
            return "head";
        } else if (startIndex < 400) {
            return "transition";
        } else {
            return "produce";
        }
    }

    /**
     * TODO: untested
     * 判断当前Msg在整个批次中的Index
     * 1. 如果当前Msg的流量累计量为0 -> 0
     * 2. 之前没有批次在生产, 或者当前批次和之前批次不一样 -> 0
     */
    private long determineIndex(OriginalMsg msg) {
        if (msg.isBatchStart() || currentBatch == null || !currentBatch.equals(msg.getBatch())) {
            currentBatch = msg.getBatch();
            startIndex = 0;
            return startIndex++;
        }
        return startIndex++;
    }

    public void execute(Tuple tuple) {
        OriginalMsg originalMsg = parseTuple(tuple);
        IoTMsg msg = new IoTMsg(originalMsg.generate());

        saveIoTMsg(msg);

        String stage = determineStage(originalMsg);
        long index = determineIndex(originalMsg);
        String brand = originalMsg.getBrand();
        String batch = originalMsg.getBatch();
        long time = originalMsg.getTimestamp();

        int[] modelConfig = getWindowSize(stage);

        window.add(msg);
        while (window.size() > modelConfig[0]) {
            window.poll();
        }
        if (window.size() == modelConfig[0]) {
            ProcessMsg processMsg = new ProcessMsg();
            processMsg.setWindowSize(modelConfig[0]);
            processMsg.setBlockSize(modelConfig[1]);
            processMsg.setIndex(index);
            processMsg.setBatch(batch);
            processMsg.setBrand(brand);
            processMsg.setStage(stage);
            processMsg.setTime(time);
            processMsg.setWindow(window);
            outputCollector.emit(new Values(processMsg));
        }
        outputCollector.ack(tuple);
    }

    private void saveIoTMsg(IoTMsg msg) {
        // TODO 保存原始数据
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("window"));
    }
}
