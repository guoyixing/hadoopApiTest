package com.gyx.hdfs.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 郭一行
 * @date 2018-09-12 09:14
 * @since 1.0.0
 */
public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //获取job信息
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);
        //获取驱动jar包
        job.setJarByClass(DistributedCacheDriver.class);
        //设置使用的Mapper
        job.setMapperClass(DistributedCacheMapper.class);
         //设置Reducer的个数为0
        job.setNumReduceTasks(0);
        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置加载缓存文件
        job.addCacheFile(new URI("file://d:/pd.txt"));
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
