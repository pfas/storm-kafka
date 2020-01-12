package core;

import msg.IoTMsg;
import msg.ProcessMsg;
import okhttp3.ResponseBody;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

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

    private int[] getWindowSize(String model) {
//        try {
//            Map<String, Object> query = new HashMap<>();
//            query.put("stage", model);
//            ResponseBody responseBody = AppUtil.doGet(
//                    AppConfig.ModelServerConfig.modelConfigUrl,
//                    new HashMap<>(),
//                    query
//            );
//            String s = responseBody.string();
//            System.out.println("responseBody:" + s);
//        } catch (IOException e) {
//            logger.error(e.getMessage());
//            e.printStackTrace();
//        }
        return new int[]{50, 5};
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


    public String determineModel(Object o) {
        // TODO determineModel
        return "head";
    }

    public void execute(Tuple tuple) {
        IoTMsg msg = parseTuple(tuple);
        String model = determineModel(tuple);
        int[] modelConfig = getWindowSize(model);

        window.add(msg);
        while (window.size() > modelConfig[0]) {
            window.poll();
        }
        if (window.size() == modelConfig[0]) {
            Object[] toProcess = new Object[3];
            toProcess[0] = modelConfig[0];
            toProcess[1] = modelConfig[1];
            toProcess[2] = window;
            ProcessMsg processMsg = new ProcessMsg();
            processMsg.setWindowSize(modelConfig[0]);
            processMsg.setBlockSize(modelConfig[1]);
            processMsg.setWindow(window);
            outputCollector.emit(new Values(processMsg));
        }
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("window"));
    }
}
