package cn.edu.ncut.storm.demo;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 将原始商品名转换成大写再发送出去
 * <p>
 * Created by Ocean lin on 2017/12/2.
 */
public class UpperBolt extends BaseBasicBolt {

    // 每收到一个消息都会执行一次
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 从tuple中获取原始商品名
        String good_name = tuple.getString(0);
        String upperCase = good_name.toUpperCase();

        // 发送出去
        basicOutputCollector.emit(new Values(upperCase));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("upper_good_name"));
    }
}
