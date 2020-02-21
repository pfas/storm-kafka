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
    private List<Double> mean;
    private List<Double> std;
    private List<Double> integral;
    private List<Double> skew;
    private List<Double> kurtosis;


    public List<Double> generate() {
        List<Double> result = new ArrayList<>();
        if (mean == null || std == null || integral == null || skew == null || kurtosis == null) {
            return result;
        }
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

    public List<Double> getMean() {
        return mean;
    }

    public void setMean(List<Double> mean) {
        this.mean = mean;
    }

    public List<Double> getStd() {
        return std;
    }

    public void setStd(List<Double> std) {
        this.std = std;
    }

    public List<Double> getIntegral() {
        return integral;
    }

    public void setIntegral(List<Double> integral) {
        this.integral = integral;
    }

    public List<Double> getSkew() {
        return skew;
    }

    public void setSkew(List<Double> skew) {
        this.skew = skew;
    }

    public List<Double> getKurtosis() {
        return kurtosis;
    }

    public void setKurtosis(List<Double> kurtosis) {
        this.kurtosis = kurtosis;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }
}
