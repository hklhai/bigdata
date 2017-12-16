package cn.edu.ncut.bigdata.advance.action;

import scala.math.Ordered;

import java.io.Serializable;
import java.util.Objects;

/**
 * 自定义二次排序key类
 * <p>
 * Created by Ocean lin on 2017/12/16.
 */
public class AccessLogKey implements Serializable, Ordered<AccessLogKey> {


    private static final long serialVersionUID = 3581404131208281985L;
    private long timeStamp;
    private long upTracffic;
    private long downTracffic;


    public AccessLogKey() {
    }

    public AccessLogKey(long timeStamp, long upTracffic, long downTracffic) {
        this.timeStamp = timeStamp;
        this.upTracffic = upTracffic;
        this.downTracffic = downTracffic;
    }

    @Override
    public boolean $greater(AccessLogKey that) {
        if (upTracffic > that.getUpTracffic()) {
            return true;
        } else if (upTracffic == that.getUpTracffic() && downTracffic > that.getDownTracffic()) {
            return true;
        } else if (upTracffic == that.getUpTracffic() && downTracffic == that.getDownTracffic() &&
                timeStamp > that.getTimeStamp()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(AccessLogKey that) {
        // 判断大于和等于
        if ($greater(that)) {
            return true;
        } else if (upTracffic == that.getUpTracffic() && downTracffic == that.getDownTracffic() &&
                timeStamp == that.getTimeStamp()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(AccessLogKey that) {
        if (upTracffic < that.getUpTracffic()) {
            return true;
        } else if (upTracffic == that.getUpTracffic() && downTracffic < that.getDownTracffic()) {
            return true;
        } else if (upTracffic == that.getUpTracffic() && downTracffic == that.getDownTracffic() &&
                timeStamp < that.getTimeStamp()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(AccessLogKey that) {
        if ($less(that)) {
            return true;
        } else if (upTracffic == that.getUpTracffic() && downTracffic == that.getDownTracffic() &&
                timeStamp == that.getTimeStamp()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(AccessLogKey that) {
        if (upTracffic - that.getUpTracffic() != 0) {
            return (int) (upTracffic - that.getUpTracffic());
        } else if (downTracffic - that.getDownTracffic() != 0) {
            return (int) (downTracffic - that.getDownTracffic());
        } else if (timeStamp - that.getTimeStamp() != 0) {
            return (int) (timeStamp - that.getTimeStamp());
        }
        return 0;
    }


    @Override
    public int compareTo(AccessLogKey that) {
        if (upTracffic - that.getUpTracffic() != 0) {
            return (int) (upTracffic - that.getUpTracffic());
        } else if (downTracffic - that.getDownTracffic() != 0) {
            return (int) (downTracffic - that.getDownTracffic());
        } else if (timeStamp - that.getTimeStamp() != 0) {
            return (int) (timeStamp - that.getTimeStamp());
        }
        return 0;
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


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccessLogKey that = (AccessLogKey) o;
        return timeStamp == that.timeStamp &&
                upTracffic == that.upTracffic &&
                downTracffic == that.downTracffic;
    }

    @Override
    public int hashCode() {

        return Objects.hash(timeStamp, upTracffic, downTracffic);
    }
}
