package com.gyx.hdfs.neicun;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-17 11:34
 * @since 1.0.0
 */
public class NeiCunBean implements Writable {
    private String brand;
    private String memory;
    private Double price;

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }


    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(brand);
        dataOutput.writeUTF(memory);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.brand = dataInput.readUTF();
        this.memory = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }
}
