package com.gyx.hdfs.neicun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDate;

public class NeiCunDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            String date = LocalDate.now().toString();
            String in = "/usr/local/python/neicun/内存" + date + ".txt";
            String out = "/usr/local/hadoopdata/neicun/"+date+"/";
            //获取job信息
            Configuration configuration = new Configuration();

            Job job = Job.getInstance(configuration);
            //获取jar包位置
            job.setJarByClass(NeiCunDriver.class);
            //关联自定义的mapper和reducer
            job.setMapperClass(NeiCunMapper.class);
            job.setReducerClass(NeiCunReducer.class);
            //设置map输出数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NeiCunBean.class);
            //设置最终传输数据类型
            job.setOutputKeyClass(OutNeiCunBean.class);
            job.setOutputValueClass(NullWritable.class);
            //设置数据输入和输出文件路径
            FileInputFormat.setInputPaths(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));

            //提交代码 等待代码提交完毕
            boolean result = job.waitForCompletion(true);
            //0是成功，1是失败
            System.exit(result ? 0 : 1);
    }

}