package com.gyx.hdfs.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-11 14:21
 * @since 1.0.0
 */
public class TableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置信息，或者job对象实例
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        //指定程序的jar包所在位置
        job.setJarByClass(TableDriver.class);
        //指定jbo要是的mapper和Reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);
        //指定mapper的输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        //指定最终输出
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);
        //指定job输入原始文件的目录和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //执行
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
