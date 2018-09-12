package com.gyx.hdfs.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 郭一行
 * @date 2018-09-12 09:49
 * @since 1.0.0
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //读取缓存文件
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt"), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割数据
            String[] fields = line.split("\t");
            //将缓存插入到集合中
            pdMap.put(fields[0], fields[1]);
        }
        //关闭流
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //切割数据
        String[] fields = line.split("\t");
        //获取pid
        String pid = fields[1];
        //根据pid获取pname
        String pName = pdMap.get(pid);
        //拼接
        line = line + "\t" + pName;
        k.set(line);
        //输出
        context.write(k, NullWritable.get());
    }
}
