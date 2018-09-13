package com.gyx.hdfs.Index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-12 15:27
 * @since 1.0.0
 */
public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //汇总
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //输出
        context.write(key, new IntWritable(sum));
    }
}
