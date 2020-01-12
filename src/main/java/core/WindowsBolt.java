package core;

import config.AppConfig;
import msg.IoTMsg;
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

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
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
        return new int[]{0, 0};
    }


    /**
     * 解析 tuple 里面的参数信息
     *
     * @param tuple
     * @return
     */
    public IoTMsg parseTuple(Tuple tuple) {
        // TODO : 解析 kafka 信息
        // String msg = (String) tuple.getValue(4);
        // System.out.println(msg);
        return new IoTMsg();
    }


    public String determineStage(Tuple tuple) {
        // TODO determineStage
        return "produce";
    }

    private long determineIndex(Tuple tuple) {
        // TODO determineIndex
        return 0;
    }

    public void execute(Tuple tuple) {
        IoTMsg msg = parseTuple(tuple);
        String stage = determineStage(tuple);
        long index = determineIndex(tuple);
        String brand = "";
        String batch = "";
        long time = 0;
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


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("window"));
    }
}
