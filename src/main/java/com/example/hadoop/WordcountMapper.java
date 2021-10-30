package com.example.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author CaiWencheng
 * @Date 2021-10-29 17:16
 */
public class WordcountMapper  extends Mapper<LongWritable, Text, Text,
        IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    /**
     * 1. map()方法中把传入的数据转为String类型
     * 2. 根据空格切分出单词
     * 3. 输出<单词，1>
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 切割
        String[] words = line.split(" ");
        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
