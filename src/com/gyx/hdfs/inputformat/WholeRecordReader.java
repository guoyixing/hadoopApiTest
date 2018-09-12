package com.gyx.hdfs.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 14:56
 * @since 1.0.0
 */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    BytesWritable bytesWritable = new BytesWritable();
    boolean isProcess = false;
    FileSplit split;
    Configuration configuration;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //读取一个一个的文件
        if (!isProcess) {
            //设置缓冲区
            byte[] buf = new byte[(int) split.getLength()];
            FileSystem fileSystem = null;
            FSDataInputStream fis = null;
            try {
                //获取文件系统
                Path path = split.getPath();
                fileSystem = path.getFileSystem(configuration);
                //打开文件输入流
                fis = fileSystem.open(path);
                //流拷贝
                IOUtils.readFully(fis, buf, 0, buf.length);
                //拷贝缓冲区的数据到最终输出
                bytesWritable.set(buf, 0, buf.length);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fileSystem);
            }
            isProcess = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProcess ? 1 : 0;
    }

    @Override
    public void close() throws IOException {
    }
}
