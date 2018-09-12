package com.gyx.hdfs.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 13:17
 * @since 1.0.0
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置信息
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包加载路径
        job.setJarByClass(OrderDriver.class);
        //加载map/reduce类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);
        //设置map输出数据key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出数据key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置reduce端分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        //设置分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
