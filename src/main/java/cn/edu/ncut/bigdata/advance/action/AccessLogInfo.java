package cn.edu.ncut.bigdata.advance.action;

import java.io.Serializable;

/**
 *
 * 访问日志信息类
 * 要求可序列化
 * Created by Ocean lin on 2017/12/15.
 */
public class AccessLogInfo implements Serializable{

    private static final long serialVersionUID = -8220675361157505871L;

    private long timeStamp;
    private long upTracffic;
    private long downTracffic;

    public AccessLogInfo(long timeStamp, long upTracffic, long downTracffic) {
        this.timeStamp = timeStamp;
        this.upTracffic = upTracffic;
        this.downTracffic = downTracffic;
    }

    public AccessLogInfo() {
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public long getUpTracffic() {
        return upTracffic;
    }

    public void setUpTracffic(long upTracffic) {
        this.upTracffic = upTracffic;
    }

    public long getDownTracffic() {
        return downTracffic;
    }

    public void setDownTracffic(long downTracffic) {
        this.downTracffic = downTracffic;
    }
}
