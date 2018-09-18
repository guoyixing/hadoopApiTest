package com.gyx.hdfs.neicun;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-17 13:29
 * @since 1.0.0
 */
public class NeiCunReducer extends Reducer<Text,NeiCunBean,OutNeiCunBean,NullWritable> {
    OutNeiCunBean k = new OutNeiCunBean();
    @Override
    protected void reduce(Text key, Iterable<NeiCunBean> values, Context context) throws IOException, InterruptedException {
        //获取内存平均价格
        Double sum = 0.0;
        int count = 0;
        Double max = 0.0;
        Double mix = 1000000000.0;
        NeiCunBean neiCunBean = new NeiCunBean();
        for (NeiCunBean value : values) {
            neiCunBean = value;
            max=max>value.getPrice()?max:value.getPrice();
            mix=mix<value.getPrice()?mix:value.getPrice();
            sum += value.getPrice();
            count++;
        }
        Double avg = sum/count;
        k.setBrand(neiCunBean.getBrand());
        k.setMemory(neiCunBean.getMemory());
        k.setAvg(avg);
        k.setMax(max);
        k.setMix(mix);
        context.write(k, NullWritable.get());
    }
}
