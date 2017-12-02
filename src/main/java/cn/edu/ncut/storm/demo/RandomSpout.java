package cn.edu.ncut.storm.demo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by Ocean lin on 2017/12/2.
 */
public class RandomSpout extends BaseRichSpout {


    SpoutOutputCollector spoutOutputCollector = null;
    String[] goods = {"iphone", "samsung", "nokia", "moto", "meizu", "lenovo"};

    // 只在开始中调用一次，初始化
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 输出的工具对象
        this.spoutOutputCollector = spoutOutputCollector;
    }

    // 获取消息并发送给下一个组件的方法,会被storm不断调用
    @Override
    public void nextTuple() {
        // 随机从goods中取出一个商品皮城发送给下一个Tuple
        Random rand = new Random();
        String good = goods[rand.nextInt(goods.length)];


        spoutOutputCollector.emit(new Values(good));
    }

    // 定义tuple的schema
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("good_name"));
    }
}
