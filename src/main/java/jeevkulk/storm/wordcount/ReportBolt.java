package jeevkulk.storm.wordcount;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ReportBolt extends BaseRichBolt {
    static Logger logger = LogManager.getLogger(BaseRichBolt.class);

    private Map<String, Long> reportCountMap = null;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.reportCountMap = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.reportCountMap.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    public void cleanup() {
        logger.info("########################## FINAL COUNTS ##########################");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.reportCountMap.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            logger.info(key + " : " + this.reportCountMap.get(key));
        }
        logger.info("########################## THE END ##########################");
    }
}