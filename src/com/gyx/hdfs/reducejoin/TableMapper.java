package com.gyx.hdfs.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-11 13:44
 * @since 1.0.0
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    TableBean v = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //区分两张表
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();

        //获取一行数据
        String line = value.toString();
        //切割数据
        String[] fields = line.split("\t");
        if (name.startsWith("order")) {
            //订单表
            v.setOrderId(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setpName("");
            v.setFlag("0");

            k.set(fields[1]);
        } else {
            //产品表
            v.setOrderId("");
            v.setPid(fields[0]);
            v.setAmount(0);
            v.setpName(fields[1]);
            v.setFlag("1");
            k.set(fields[0]);
        }
        context.write(k, v);
    }
}
