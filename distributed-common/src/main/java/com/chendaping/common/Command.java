package com.chendaping.common;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by chendaping on 2018/12/8.
 */
public class Command {
    // 注册服务端
    public final static int REGISTER_SERVER = 1;
    // 取消注册
    public final static int UNREGISTER_SERVER = 2;
    // 调用方法
    public final static int INVOKE_REQUEST = 3;
    // 方法返回
    public final static int INVOKE_RESPONSE = 4;
    // 获取服务列表
    public final static int GET_SERVER_LIST = 5;
    public final static int GET_SERVER_LIST_RESPONSE = 6;

    private static AtomicLong IDS = new AtomicLong(0);

    private int type;
    private long requestId;

    // 存放具体的方法调用信息、调用结果等
    private byte[] body;

    public Command(int type, byte[] body) {
        this.type = type;
        this.body = body;
        this.requestId = IDS.incrementAndGet();
    }

    // command 长度 = type(4) + requestId(8) + body.length
    public int length() {
        return 12 + (body == null ? 0 : body.length);
    }

    public int getType() {
        return type;
    }
    public void setType(int type) {
        this.type = type;
    }
    public byte[] getBody() { return body; }
    public void setBody(byte[] body) { this.body = body; }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }
}