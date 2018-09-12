package com.gyx.hdfs.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable 输入的key 行号
 * Text 输入的Value 每行的内容
 * Text 输出的key 单词
 * IntWritable 输出的Value 每个单词的个数
 * @date 2018-09-05 13:41
 * @since 1.0.0
 */
public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    /**
     * 重写map方法
     * 每对KV都会被执行一次map
     */
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    //	hello world
//	atguigu atguigu
//	hadoop
//	spark
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1 一行内容转换成string
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 循环写出到下一个阶段
        for (String word : words) {

            k.set(word);

            context.write(k, v);
        }
    }
}
