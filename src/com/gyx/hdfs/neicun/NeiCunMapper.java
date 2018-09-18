package com.gyx.hdfs.neicun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 郭一行
 * @date 2018-09-17 11:09
 * @since 1.0.0
 */
public class NeiCunMapper extends Mapper<LongWritable,Text,Text,NeiCunBean> {
    Text k=new Text();
    NeiCunBean neiCunBean = new NeiCunBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一条数据
        String line = value.toString();
        if (!line.contains("单套容量")){
            return;
        }
        //切割数据
        String[] data = line.split("\t");
        //创建对象

        neiCunBean.setPrice(Double.parseDouble(data[1]));
        neiCunBean.setBrand(data[2]);
        for (String datum : data) {
            if (datum.contains("单套容量")){
                String regEx="[^0-9]";
                Pattern p = Pattern.compile(regEx);
                Matcher m = p.matcher(datum);
                neiCunBean.setMemory(m.replaceAll("").trim());
            }
        }
        k.set(neiCunBean.getBrand() + neiCunBean.getMemory());
        //输出
        context.write(k,neiCunBean);
    }
}
