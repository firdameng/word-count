package com.example.hadoop.speak;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;

/**
 * @Author CaiWencheng
 * @Date 2021-11-01 11:25
 */
public class SpeakBean implements Writable {

    private Long selfDuration;
    private Long thirdPartDuration;
    private String deviceId;
    private Long sumDuration;

    /**
     * 反序列化时，需要反射调用空参构造函数，所以必须有
     */
    public SpeakBean(){

    }

    public SpeakBean(Long selfDuration, Long thirdPartDuration,String deviceId) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = this.selfDuration + this.thirdPartDuration;
    }

    /**
     * 为了方便观察数据，
     * @return
     */
    @Override
    public String toString() {
        return selfDuration +
                "\t" + thirdPartDuration +
                "\t" + deviceId +
                "\t" + sumDuration;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }

    /**
     * 序列化方法,把内容输出到网络或文本中
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceId);
        out.writeLong(sumDuration);
    }

    /**
     * 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDuration = in.readLong();
        this.thirdPartDuration = in.readLong();
        this.deviceId = in.readUTF();
        this.sumDuration = in.readLong();
    }
}
