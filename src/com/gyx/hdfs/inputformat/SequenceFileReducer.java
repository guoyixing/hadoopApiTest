package com.gyx.hdfs.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 15:32
 * @since 1.0.0
 */
public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable>{
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable value : values) {
            context.write(key,value);
        }
    }
}
