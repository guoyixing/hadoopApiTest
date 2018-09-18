package com.gyx.hdfs.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordcountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //获取job信息
        Configuration configuration = new Configuration();
        //开启map端的压缩
        configuration.setBoolean("mapreduce.map.output.compress",true);
        //设置map端输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);
        //获取jar包位置
        job.setJarByClass(WordcountDriver.class);
        //关联自定义的mapper和reducer
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        //设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终传输数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置数据输入和输出文件路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job,true);
        //设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);

        //指定Combiner
        job.setCombinerClass(WordcountCombiner.class);
        //提交代码 等待代码提交完毕
        boolean result = job.waitForCompletion(true);
        //0是成功，1是失败
        System.exit(result?0:1);
    }
}
