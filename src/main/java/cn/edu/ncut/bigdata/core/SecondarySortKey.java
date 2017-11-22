package cn.edu.ncut.bigdata.core;

import scala.Serializable;
import scala.math.Ordered;

/**
 * Created by Ocean lin on 2017/10/18.
 * <p>
 * 需要自定义二次排序的key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    private int first;

    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if (this.first < that.first)
            return true;
        if (this.first == that.first && this.second < that.second)
            return true;
        else
            return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.first)
            return true;
        if (this.first == that.first && this.second > that.second)
            return true;
        else
            return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that))
            return true;
        if (this.first == that.first && this.second == that.second)
            return true;
        else
            return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        //借用上面的$greater
        if (this.$greater(that))
            return true;
        if (this.first == that.first && this.second == that.second)
            return true;
        else
            return false;
    }


    @Override
    public int compare(SecondarySortKey that) {
        if (this.first - that.first != 0)
            return this.first - that.first;
        else
            return this.second - that.second;

    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if (this.first - that.first != 0)
            return this.first - that.first;
        else
            return this.second - that.second;
    }

    //为要进行排序的多个列，提供getter和setter方法，以及hashcode和equals方法
        public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }
}
