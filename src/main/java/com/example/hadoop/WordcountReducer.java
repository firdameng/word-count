package com.example.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author CaiWencheng
 * @Date 2021-10-29 17:21
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text,
        IntWritable> {
    int sum;
    IntWritable v = new IntWritable();

    /**
     * Reduce阶段：
     * 1. 汇总各个key(单词)的个数，遍历value数据进行累加
     * 2. 输出key的总数
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出
        v.set(sum);
        context.write(key,v);
    }
}
