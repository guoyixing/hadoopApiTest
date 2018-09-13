package com.gyx.hdfs.Index;

import com.gyx.hdfs.flowsort.FlowBean;
import com.gyx.hdfs.flowsort.FlowSortDriver;
import com.gyx.hdfs.flowsort.FlowSortMapper;
import com.gyx.hdfs.flowsort.FlowSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-12 15:32
 * @since 1.0.0
 */
public class OneIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar包路径
        job.setJarByClass(OneIndexDriver.class);

        // 3 管理mapper和reducer类
        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReducer.class);

        // 4 设置mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
