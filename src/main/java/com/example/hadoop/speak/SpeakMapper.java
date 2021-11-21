package com.example.hadoop.speak;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 4个参数，分为2对kv
 * 第一对kv, mapper输入参数的kv类型 k -->一行文本的偏移量，v-->一行文本内容
 * 第二对kv,mapper输出参数的kv类型， k--> mapper输出的key类型，v-->mapper输出的value类型
 * @Author CaiWencheng
 * @Date 2021-11-01 11:25
 */
public class SpeakMapper extends Mapper<LongWritable, Text,Text,SpeakBean> {
    /*
    1. 转换接收到text数据为 string
    2. 按制表符切分，得到自有内容时长，第三方内容时长，设备id
    3. 直接输出：key -->设备id, value- 》 speakbean
     */
    //Text device_id = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String selfDuration = fields[fields.length - 3];
        String thirdPartDuration = fields[fields.length - 2];
        String deviceId = fields[1];
        SpeakBean speakBean = new SpeakBean(Long.parseLong(selfDuration), Long.parseLong(thirdPartDuration), deviceId);
       // device_id.set(deviceId);
        context.write(new Text(deviceId),speakBean);
    }
}
