package msg;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OriginalMsg implements Serializable {

    public class SensorMsg {
        Object id;
        Object v;
        // Object q;
        // Object t;
    }


    private static final Map<String, String> mapping = new HashMap<>();

    private static String getKey(String value) {
        String key = "";
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            if (value.equals(entry.getValue())) {
                key = entry.getKey();
            }
        }
        return key;
    }

    static {
        mapping.put("deviceStatus1", "5H.5H.K1_Z7TT1_PHASE1");
        mapping.put("deviceStatus2", "5H.5H.K1_Z7TT1_PHASE1");

        mapping.put("batch", "6032.6032.YS2CUTBATCHNO");
        mapping.put("brand", "6032.6032.YS2CUTBRAND");

        mapping.put("flowAcc", "5H.5H.YT6032_CK_ACCU_PV");
        mapping.put("humidOut", "5H.5H.K1_Z7ZF2_FEED_MOIST_SP_SPAN");
        mapping.put("humidIn", "5H.5H.K1_Z7ZF1_FEED_MOIST_SP_SPAN");
        mapping.put("humidSetting", "5H.5H.K1_Z7TT1VALUE_20");
        mapping.put("windSpeed", "5H.5H.K1_Z7TT1VALUE_24");
        mapping.put("tempSetting1", "5H.5H.K1_Z7TT1VALUE_1");
        mapping.put("tempActual1", "5H.5H.K1_Z7TT1VALUE_2");
        mapping.put("tempSetting2", "5H.5H.K1_Z7TT1VALUE_4");
        mapping.put("tempActual2", "5H.5H.K1_Z7TT1VALUE_5");
        mapping.put("press", "5H.5H.K1_Z7TT1VALUE_29");

    }

    private long timestamp;
    private List<SensorMsg> values;

    public OriginalMsg(long timestamp, List<SensorMsg> values) {
        this.timestamp = timestamp;
        this.values = values;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isBatchStart() {
        if (values.size() == 0) {
            return false;
        }
        for (SensorMsg sensorMsg : values) {
            if (sensorMsg.id instanceof String && sensorMsg.id.equals(mapping.get("flowAcc"))) {
                return (double) sensorMsg.v == 0;
            }
        }
        return false;
    }

    public String getBrand() {
        if (values.size() == 0) {
            return "";
        }
        for (SensorMsg sensorMsg : values) {
            if (sensorMsg.id instanceof String && sensorMsg.id.equals(mapping.get("brand"))) {
                return (String) sensorMsg.v;
            }
        }
        return "";
    }

    public String getBatch() {
        if (values.size() == 0) {
            return "";
        }
        for (SensorMsg sensorMsg : values) {
            if (sensorMsg.id instanceof String && sensorMsg.id.equals(mapping.get("batch"))) {
                return (String) sensorMsg.v;
            }
        }
        return "";
    }

    public Double[] generate() {
        if (values.size() == 0) {
            return new Double[]{};
        }
        Map<String, Double> result = new HashMap<>();

        int count = 0;
        for (SensorMsg sensorMsg : values) {
            if (sensorMsg.id instanceof String && mapping.containsValue(sensorMsg.id)) {
                count++;
                if (((String) sensorMsg.id).startsWith("5H.5H")) {
                    result.put(getKey((String) sensorMsg.id), (Double) sensorMsg.v);
                }
            }
        }
        if (count != mapping.size()) {
            return new Double[]{};
        }
        return new Double[]{
                result.get("humidOut") - result.get("humidSetting"),
                result.get("windSpeed"),
                result.get("humidOut"),
                result.get("humidIn"),
                result.get("press"),
                result.get("tempActual1"),
                result.get("tempSetting1"),
                result.get("tempActual2"),
                result.get("tempSetting2"),
        };
    }


}
