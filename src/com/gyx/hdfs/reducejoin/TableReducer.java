package com.gyx.hdfs.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 郭一行
 * @date 2018-09-11 14:00
 * @since 1.0.0
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //准备集合
        List<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();
        //数据拷贝
        for (TableBean value : values) {
            if ("0".equals(value.getFlag())) {
                //订单表
                TableBean tableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tableBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tableBean);
            } else {
                //产品表
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //拼接表
        for (TableBean orderBean : orderBeans) {
            orderBean.setpName(pdBean.getpName());
            //输出
            context.write(orderBean, NullWritable.get());
        }
    }
}
