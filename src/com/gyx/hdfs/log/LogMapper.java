package com.gyx.hdfs.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-12 13:23
 * @since 1.0.0
 */
public class LogMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取1行
        String line = value.toString();
        //分析日志是否合法
        LogBean bean = pressLog(line);
        if (!bean.isValid()){
            context.getCounter("map","false").increment(1);
            return;
        }
        context.getCounter("map","true").increment(1);
        k.set(bean.toString());
        //输出
        context.write(k,NullWritable.get());
    }

    private LogBean pressLog(String line) {
        LogBean logBean = new LogBean();
        //截取
        String[] fields = line.split(" ");
        if (fields.length>1){
            //封装数据
            logBean.setRemoteAddr(fields[0]);
            logBean.setRemoteUser(fields[1]);
            logBean.setTimeLocal(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBodyBytesSent(fields[9]);
            logBean.setHttpReferer(fields[10]);
            if (fields.length>12) {
                logBean.setHttpUserAgent(fields[11]+""+fields[12]);
            }else {
                logBean.setHttpUserAgent(fields[11]);
            }
            //大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus())>=400){
                logBean.setValid(false);
            }
        }else {
            logBean.setValid(false);
        }
        return logBean;
    }
}
