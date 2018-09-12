package com.gyx.hdfs.log;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 郭一行
 * @date 2018-09-12 13:12
 * @since 1.0.0
 */
public class LogBean{
    /**
     * 客户端ip地址
     */
    private String remoteAddr;
    /**
     * 用户名称
     */
    private String remoteUser;
    /**
     * 访问时间和访问时区
     */
    private String timeLocal;
    /**
     * 请求的url和http协议
     */
    private String request;
    /**
     * 请求状态
     */
    private String status;
    /**
     * 发送给客户端文件主题内容大小
     */
    private String bodyBytesSent;
    /**
     * 从哪个页面链接访问过来的
     */
    private String httpReferer;
    /**
     * 客户浏览器的相关信息
     */
    private String httpUserAgent;
    /**
     * 判断数据是否合法
     */
    private boolean valid = true;

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBodyBytesSent() {
        return bodyBytesSent;
    }

    public void setBodyBytesSent(String bodyBytesSent) {
        this.bodyBytesSent = bodyBytesSent;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    public void setHttpReferer(String httpReferer) {
        this.httpReferer = httpReferer;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "remoteAddr='" + remoteAddr + '\'' +
                ", remoteUser='" + remoteUser + '\'' +
                ", timeLocal='" + timeLocal + '\'' +
                ", request='" + request + '\'' +
                ", status='" + status + '\'' +
                ", bodyBytesSent='" + bodyBytesSent + '\'' +
                ", httpReferer='" + httpReferer + '\'' +
                ", httpUserAgent='" + httpUserAgent + '\'' +
                ", valid=" + valid +
                '}';
    }
}
