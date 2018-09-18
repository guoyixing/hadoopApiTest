package com.gyx.hdfs.neicun;

import com.google.gson.Gson;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-17 11:34
 * @since 1.0.0
 */
public class OutNeiCunBean implements Writable {
    private int id;
    private String brand;
    private String memory;
    private Double avg;
    private Double max;
    private Double mix;
    private String date;

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

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public Double getMix() {
        return mix;
    }

    public void setMix(Double mix) {
        this.mix = mix;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(brand);
        dataOutput.writeUTF(memory);
        dataOutput.writeDouble(avg);
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(mix);
        dataOutput.writeUTF(date);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.brand = dataInput.readUTF();
        this.memory = dataInput.readUTF();
        this.avg = dataInput.readDouble();
        this.max = dataInput.readDouble();
        this.mix = dataInput.readDouble();
        this.date = dataInput.readUTF();
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
