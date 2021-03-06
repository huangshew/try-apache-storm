package t.r.y.storm.components;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class TextFileSpout implements IRichSpout {

    private BufferedReader reader;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        try {
            this.reader = new BufferedReader(
                    new FileReader(
                            new File(
                                    "/Users/shenglx/dev/try-apache-storm/word-count/src/main/resources/storm.txt")
                    ));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (this.reader == null) {
            try {
                this.reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if (this.reader == null) {
            return;
        }

        try {
            String nextLine = this.reader.readLine();
            if (nextLine != null && nextLine.trim().length() > 0) {
                this.collector.emit(new Values(nextLine));
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.LINE));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
