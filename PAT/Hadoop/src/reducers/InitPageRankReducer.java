/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
/**
 *
 * @author FiqieUlya
 */
public class InitPageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        boolean empty = true;
        //key: f1, value: u1
        while (values.hasNext()) {
            if (!empty) {
		sb.append(",");
            }
            empty = false;
            sb.append(values.next().toString());
            count++;
        }
        sb.append(","+count+","+1);
	oc.collect(key, new Text(sb.toString()));
        // output: f1[ ]u1,u2,u3,3,PR 
    }
}
