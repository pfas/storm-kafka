package msg;

import java.io.Serializable;
import java.util.List;

public class ProcessMsg implements Serializable {

    private String stage;
    private String brand;
    private String batch;
    private long time;
    private long index;

    private int windowSize;
    private int blockSize;
    private List<IoTMsg> window;

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public List<IoTMsg> getWindow() {
        return window;
    }

    public void setWindow(List<IoTMsg> window) {
        this.window = window;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getBatch() {
        return batch;
    }

    public void setBatch(String batch) {
        this.batch = batch;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }
}
