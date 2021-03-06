package t.r.y.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import t.r.y.storm.components.SplitSentenceBolt;
import t.r.y.storm.components.TextFileSpout;
import t.r.y.storm.components.WordCountBolt;

public class WordCountTopo {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TextFileSpout(), 5);
        builder.setBolt("split", new SplitSentenceBolt(), 8)
                .shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 12)
                .fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
