/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop;

import drivers.CalculatePageRank;
import drivers.FinishPageRank;
import drivers.InitPageRank;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author FiqieUlya
 */
public class Main2 {
    static final int NUMBER_OF_ITERATION = 3;
            
	public static void main(String[] args) throws Exception {

                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);

                String[] prepareOpts = {args[0], "pr-0.out"};
		ToolRunner.run(new Configuration(), new InitPageRank(), prepareOpts);

				
		for (int i = 1; i <= NUMBER_OF_ITERATION; i++) {
                        String previous = "pr-" + (i - 1) + ".out";
                        String current = "pr-" + i + ".out";
		 	String[] opts = {previous, current};
		 	ToolRunner.run(new Configuration(), new CalculatePageRank(), opts);
                        
                        if (i == NUMBER_OF_ITERATION) {
                            String[] finalOpts = {"pr-3.out", args[1]};
                            ToolRunner.run(new Configuration(), new FinishPageRank(), finalOpts);
                        }
		}
	}
}
