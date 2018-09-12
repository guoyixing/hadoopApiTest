package com.gyx.hdfs.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 13:16
 * @since 1.0.0
 */
public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
