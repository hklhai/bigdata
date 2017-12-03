package cn.edu.ncut.storm.demo;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * 描述topology的结构，以及创建topology并提交给集群
 * Created by Ocean lin on 2017/12/2.
 */
public class MainTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // (唯一名称,实例，并发度)
        topologyBuilder.setSpout("RandomSpout", new RandomSpout(), 4);

        // shuffleGrouping设置Spout与Bolt之间关系
        topologyBuilder.setBolt("UpperBolt", new UpperBolt(), 4).
                shuffleGrouping("RandomSpout");

        topologyBuilder.setBolt("SuffixBolt", new SuffixBolt(), 4)
                .shuffleGrouping("UpperBolt");

        // 创建topology
        StormTopology topology = topologyBuilder.createTopology();

        Config config = new Config();
        config.setNumWorkers(4);
        config.setDebug(true);
        // 设置消息应答器
        config.setNumAckers(0);

        // 提交topology至storm集群
        StormSubmitter.submitTopologyWithProgressBar("Test_Storm", config, topology);

    }
}
