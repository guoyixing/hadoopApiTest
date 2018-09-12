package com.gyx.hdfs.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Text 输入的key 单词
 * IntWritable 输入的Value 每个单词的个数
 * Text 输出的key 单词
 * IntWritable 输出的Value 每个单词的个数
 *
 * @date 2018-09-05 14:25
 * @since 1.0.0
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * 重写reduce方法
     * ReduceTask进程对每一组相同K的KV对调用一次reduce()方法
     * 例如：从mapper传来的数据如下
     * hello 1
     * hello 1
     * world 1
     * world 1
     * 这时候会把key为hello的数据做成一组，value便是1,1
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

