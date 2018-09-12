package com.gyx.hdfs.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-11 10:23
 * @since 1.0.0
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private Configuration configuration;
    private FSDataOutputStream comFs = null;
    private FSDataOutputStream otherFs = null;
    public FilterRecordWriter() {
    }

    public FilterRecordWriter(TaskAttemptContext job){
        configuration = job.getConfiguration();
        //获取文件系统
        FileSystem fileSystem = null;

        try {
            fileSystem = FileSystem.get(configuration);
            //创建两个输出流
            comFs = fileSystem.create(new Path("d:/com.log"));
            otherFs = fileSystem.create(new Path("d:/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //判断数据是否包含com
        if (text.toString().contains("com")){
            comFs.write(text.getBytes());
        }else {
            otherFs.write(text.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关闭流
        if (comFs !=null){
            comFs.close();
        }
        if (otherFs !=null){
            otherFs.close();
        }
    }
}
