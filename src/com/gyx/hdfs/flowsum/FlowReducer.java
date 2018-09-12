package com.gyx.hdfs.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sumUpFlow = 0;
        long sumDownFlow = 0;

        // 1 累加求和
        for (FlowBean flowBean : values) {
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        }

        FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);

        // 2 输出
        context.write(key, flowBean);
    }
}
