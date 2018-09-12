package com.gyx.hdfs.outputformat;

import com.gyx.hdfs.inputformat.SequenceFileDriver;
import com.gyx.hdfs.inputformat.SequenceFileMapper;
import com.gyx.hdfs.inputformat.SequenceFileReducer;
import com.gyx.hdfs.inputformat.WholeFileInputformat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-11 10:51
 * @since 1.0.0
 */
public class FilterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置信息
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包加载路径
        job.setJarByClass(FilterDriver.class);
        //加载map/reduce类
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        //设置OutFormat
        job.setOutputFormatClass(FilterOutputFormat.class);
        //设置map输出数据key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出数据key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
