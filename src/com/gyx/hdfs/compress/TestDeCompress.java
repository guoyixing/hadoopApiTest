package com.gyx.hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.*;

/**
 * @author 郭一行
 * @date 2018-09-13 15:45
 * @since 1.0.0
 */
public class TestDeCompress {
    public static void main(String[] args) throws IOException {
        decompress("d:/hello.txt.bz2");
    }

    private static void decompress(String fileName) throws IOException {
        //校验文件
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));
        if (codec == null){
            //不支持解码
            return;
        }
        //获取输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(fileName)));
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

        //流对拷
        IOUtils.copyBytes(cis,fos,1024*1024*5,false);
        //关闭流
        cis.close();
        fos.close();
    }
}
