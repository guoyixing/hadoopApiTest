package com.gyx.hdfs.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 1 获取job对象
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置jar包路径
		job.setJarByClass(FlowDriver.class);

		// 3 管理mapper和reducer类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		// 4 设置mapper输出的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

//		// 设置分区
		job.setPartitionerClass(ProvincePartitioner.class);
		/*
		NumReduceTasks是生成文件的个数  最好等于分类的个数
		等于1则没有效果
		大于1小于分类个数则会报错
		大约分类个数则会出现空文件
		 */
		job.setNumReduceTasks(6);
		
		// 6 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
