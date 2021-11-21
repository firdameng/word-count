package com.example.hadoop.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author CaiWencheng
 * @Date 2021-11-01 11:55
 */
public class SpeakReducer extends Reducer<Text,SpeakBean,Text,SpeakBean> {

    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        Long self_duration = 0L;
        Long third_part_duration = 0L;
        for(SpeakBean bean:values){
            self_duration+= bean.getSelfDuration();
            third_part_duration+=bean.getThirdPartDuration();
        }

        // 输出，封装成bean对象输出
        SpeakBean bean = new SpeakBean(self_duration, third_part_duration, key.toString());
        context.write(key,bean);
    }
}
