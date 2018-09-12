package com.gyx.hdfs.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 郭一行
 * @date 2018-09-10 13:10
 * @since 1.0.0
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return (orderBean.getOrderId() & Integer.MAX_VALUE) % i;
    }
}
