package com.gyx.hdfs.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-11 10:44
 * @since 1.0.0
 */
public class FilterMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //设置key
        k.set(line);
        //输出
        context.write(k,NullWritable.get());
    }
}
