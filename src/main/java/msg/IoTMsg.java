package msg;

import java.util.Arrays;

public class IoTMsg {

//    public static final String HUMID_OUT_DIFF = "humidOutDiff";
//    public static final String WIND_SPEED = "windspeedActual";
//    public static final String HUMID_IN = "humidIn";
//    public static final String PRESS = "press";
//    public static final String TEMP_ACTUAL_1 = "tempRegion1Actual";
//    public static final String TEMP_SETTING_1 = "tempRegion1Setting";
//    public static final String TEMP_ACTUAL_2 = "tempRegion2Actual";
//    public static final String TEMP_SETTING_2 = "tempRegion2Setting";
//
//    public static final String HUMID_OUT = "humidOut";

    private float humidOutDiff;
    private float windSpeed;
    private float humidIn;
    private float press;
    private float tempActual1;
    private float tempSetting1;
    private float tempActual2;
    private float tempSetting2;

    public IoTMsg() {
    }

    public IoTMsg(Float[] array) {
        this.humidOutDiff = array[0];
        this.windSpeed = array[1];
        this.humidIn = array[2];
        this.press = array[3];
        this.tempActual1 = array[4];
        this.tempSetting1 = array[5];
        this.tempActual2 = array[6];
        this.tempSetting2 = array[7];
    }

    public IoTMsg(float humidOutDiff,
                  float windSpeed,
                  float humidIn,
                  float press,
                  float tempActual1,
                  float tempSetting1,
                  float tempActual2,
                  float tempSetting2
    ) {
        this.humidOutDiff = humidOutDiff;
        this.windSpeed = windSpeed;
        this.humidIn = humidIn;
        this.press = press;
        this.tempActual1 = tempActual1;
        this.tempSetting1 = tempSetting1;
        this.tempActual2 = tempActual2;
        this.tempSetting2 = tempSetting2;
    }


    public Float[] generate() {
        return new Float[]{
                getHumidOutDiff(),
                getWindSpeed(),
                getHumidIn(),
                getPress(),
                getTempActual1(),
                getTempSetting1(),
                getTempActual2(),
                getTempSetting2()
        };
    }

    public float getHumidOutDiff() {
        return humidOutDiff;
    }

    public void setHumidOutDiff(float humidOutDiff) {
        this.humidOutDiff = humidOutDiff;
    }

    public float getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(float windSpeed) {
        this.windSpeed = windSpeed;
    }

    public float getHumidIn() {
        return humidIn;
    }

    public void setHumidIn(float humidIn) {
        this.humidIn = humidIn;
    }

    public float getPress() {
        return press;
    }

    public void setPress(float press) {
        this.press = press;
    }

    public float getTempActual1() {
        return tempActual1;
    }

    public void setTempActual1(float tempActual1) {
        this.tempActual1 = tempActual1;
    }

    public float getTempSetting1() {
        return tempSetting1;
    }

    public void setTempSetting1(float tempSetting1) {
        this.tempSetting1 = tempSetting1;
    }

    public float getTempActual2() {
        return tempActual2;
    }

    public void setTempActual2(float tempActual2) {
        this.tempActual2 = tempActual2;
    }

    public float getTempSetting2() {
        return tempSetting2;
    }

    public void setTempSetting2(float tempSetting2) {
        this.tempSetting2 = tempSetting2;
    }


    @Override
    public String toString() {
        return Arrays.toString(generate());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IoTMsg)) {
            return false;
        }
        Float[] source = generate();
        Float[] target = ((IoTMsg) obj).generate();
        if (source.length != target.length) {
            return false;
        }
        for (int i = 0; i < source.length; i++) {
            if (!source[i].equals(target[i])) {
                return false;
            }
        }
        return true;

    }
}
