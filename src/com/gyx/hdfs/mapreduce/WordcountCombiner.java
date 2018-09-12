package com.gyx.hdfs.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 09:53
 * @since 1.0.0
 */
public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //合并汇总
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //输出
        context.write(key,new IntWritable(sum));
    }
}
