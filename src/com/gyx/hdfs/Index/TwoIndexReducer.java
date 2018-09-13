package com.gyx.hdfs.Index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-12 15:42
 * @since 1.0.0
 */
public class TwoIndexReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //拼接
        StringBuilder sb =new StringBuilder();
        for (Text value : values) {
            sb.append(value.toString().replace("\t","-->")+"\t");
        }
        //输出
        context.write(key,new Text(sb.toString()));
    }
}
