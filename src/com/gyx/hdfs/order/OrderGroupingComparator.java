package com.gyx.hdfs.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author 郭一行
 * @date 2018-09-10 13:35
 * @since 1.0.0
 */
public class OrderGroupingComparator extends WritableComparator {
    /**
     * 分组的时候必须有个构造函数
     */
    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        //id相同就任务是同一个对象
        int result;
        if (aBean.getOrderId() > bBean.getOrderId()) {
            result = 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }
}
