package com.gyx.hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @author 郭一行
 * @date 2018-09-13 15:27
 * @since 1.0.0
 */
public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //测试压缩
        compress("d:/hello.txt","org.apache.hadoop.io.compress.Bzip2Codec");
    }

    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        //获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File(fileName));
        //反射获取压缩类的信息
        Class<?> className = Class.forName(method);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(className, new Configuration());
        //获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fileOutputStream);
        //流的对拷
        IOUtils.copyBytes(fileInputStream,cos,1024*1024*5,false);
        //关闭流
        fileInputStream.close();
        cos.close();
        fileOutputStream.close();
    }
}
