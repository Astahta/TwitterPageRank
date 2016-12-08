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
public class CalculatePageRank1Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> oc, Reporter rprtr) throws IOException {
        double PR = 0;
        StringBuilder sb = new StringBuilder();
        //key:u1   value:0.85*PR/3
        //key:f1   value:user,u1,u2,u3,3,PR 
        while (values.hasNext()) {
            String temp = values.next().toString();
            if(temp.contains("user")){
                String[] users = temp.split(",");
                for(int i = 1; i < users.length-1; i++){
                    sb.append(users[i]);
                    sb.append(",");
                }
            } else{
                PR+=Double.parseDouble(temp);
            }
        }
        sb.append(PR + 0.15);
	oc.collect(key, new Text(sb.toString()));
        //output: fi[ ]u1,u2,u3,3,PR 
    }
    
}
