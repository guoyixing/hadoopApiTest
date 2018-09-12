package com.gyx.hdfs.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 11:28
 * @since 1.0.0
 */
public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
    OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //切割
        String[] fields = line.split("\t");
        //封装对象
        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));
        //输出
        context.write(k,NullWritable.get());
    }
}
