package msg;

import java.io.Serializable;
import java.util.Queue;

public class ProcessMsg implements Serializable {

    private String model;
    private int windowSize;
    private int blockSize;
    private Queue<Object> window;

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
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

    public Queue<Object> getWindow() {
        return window;
    }

    public void setWindow(Queue<Object> window) {
        this.window = window;
    }
}
