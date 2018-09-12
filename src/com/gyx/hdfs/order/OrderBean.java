package com.gyx.hdfs.order;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-10 11:09
 * @since 1.0.0
 */
public class OrderBean implements WritableComparable<OrderBean> {
    /**
     * 订单id
     */
    private int orderId;
    /**
     * 价格
     */
    private double price;

    public OrderBean(int orderId, int price) {
        this.orderId = orderId;
        this.price = price;
    }

    public OrderBean() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if (orderId > o.getOrderId()) {
            result = 1;
        } else if (orderId < o.getOrderId()) {
            result = -1;
        } else {
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    @Override
    public String toString() {
        return orderId + "/t" + price;
    }
}
