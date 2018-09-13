package com.gyx.hdfs.Index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-12 14:58
 * @since 1.0.0
 */
public class OneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    String name;
    private Text k = new Text();
    private IntWritable v = new IntWritable();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //切割
        String[] fields = line.split(" ");
        for (String field : fields) {
            //拼接
            k.set(field+"--"+name);
            //输出
            context.write(k,v);
        }
    }
}
