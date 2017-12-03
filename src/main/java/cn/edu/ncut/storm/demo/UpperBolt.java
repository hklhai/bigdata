package cn.edu.ncut.storm.demo;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * 添加后缀，写入文件中
 * Created by Ocean lin on 2017/12/2.
 */
public class UpperBolt extends BaseBasicBolt {

    FileWriter fileWriter = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        try {
            this.fileWriter = new FileWriter("/home/root" + UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String upper_good_name = tuple.getString(0);
        String final_good_name = upper_good_name + "-Storm Goods";

        try {
            fileWriter.append(final_good_name);
            fileWriter.append("\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("final_good_name"));
    }
}
