package msg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ModelMsg implements Serializable {
    private long time;
    private String batch;
    private long index;
    private String brand;
    private String stage;
    private List<Float> mean;
    private List<Float> std;
    private List<Float> integral;
    private List<Float> skew;
    private List<Float> kurtosis;


    public List<Float> generate() {
        List<Float> result = new ArrayList<>();
        result.addAll(mean);
        result.addAll(std);
        result.addAll(integral);
        result.addAll(skew);
        result.addAll(kurtosis);
        return result;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getBatch() {
        return batch;
    }

    public void setBatch(String batch) {
        this.batch = batch;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public List<Float> getMean() {
        return mean;
    }

    public void setMean(List<Float> mean) {
        this.mean = mean;
    }

    public List<Float> getStd() {
        return std;
    }

    public void setStd(List<Float> std) {
        this.std = std;
    }

    public List<Float> getIntegral() {
        return integral;
    }

    public void setIntegral(List<Float> integral) {
        this.integral = integral;
    }

    public List<Float> getSkew() {
        return skew;
    }

    public void setSkew(List<Float> skew) {
        this.skew = skew;
    }

    public List<Float> getKurtosis() {
        return kurtosis;
    }

    public void setKurtosis(List<Float> kurtosis) {
        this.kurtosis = kurtosis;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }
}
