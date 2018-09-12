package com.gyx.hdfs.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-11 10:20
 * @since 1.0.0
 */
public class FilterOutputFormat extends FileOutputFormat<Text,NullWritable>{
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FilterRecordWriter(taskAttemptContext);
    }
}
