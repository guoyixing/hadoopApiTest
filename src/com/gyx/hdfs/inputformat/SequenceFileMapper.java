package com.gyx.hdfs.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 15:24
 * @since 1.0.0
 */
public class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片信息
        FileSplit split = (FileSplit)context.getInputSplit();
        //获取文件的路径和文件名称
        Path path = split.getPath();
        k.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(k,value);
    }
}
