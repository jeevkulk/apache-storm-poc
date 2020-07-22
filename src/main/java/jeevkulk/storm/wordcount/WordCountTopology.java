package jeevkulk.storm.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.StormSubmitter;



public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "jeevkulk.storm.wordcount.SentenceSpout";
	private static final String SPLIT_BOLT_ID = "jeevkulk.storm.wordcount.SplitSentenceBolt";
	private static final String COUNT_BOLT_ID = "jeevkulk.storm.wordcount.WordCountBolt";
	private static final String REPORT_BOLT_ID = "jeevkulk.storm.wordcount.ReportBolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.OFF);

		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);
		// jeevkulk.storm.wordcount.SentenceSpout --> jeevkulk.storm.wordcount.SplitSentenceBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).shuffleGrouping(SENTENCE_SPOUT_ID);
		// jeevkulk.storm.wordcount.SplitSentenceBolt --> jeevkulk.storm.wordcount.WordCountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("words"));
		// jeevkulk.storm.wordcount.WordCountBolt --> jeevkulk.storm.wordcount.ReportBolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt, 1).globalGrouping(COUNT_BOLT_ID);

		Config config = new Config();

		if (args != null && args.length > 0) {
			config.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			try {
				cluster.submitTopology("test", config, builder.createTopology());
				Thread.sleep(100000);
				cluster.killTopology("test");
				cluster.shutdown();
				System.exit(0);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
	}
}