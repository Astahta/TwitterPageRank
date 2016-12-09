/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package reducers;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author FiqieUlya
 */
public class SortingPageRankReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable>{

     public int maxoutput =5; 
    public int count =0; 
    @Override 
    public void reduce(DoubleWritable k2, Iterator<Text> itrtr, OutputCollector<Text, DoubleWritable> oc, Reporter rprtr) throws IOException { 
        Iterator<Text> it = itrtr; 
        while (it.hasNext()) { 
            oc.collect(it.next(), k2); 
            count++; 
            if(count>=5) 
                break; 
        } 
    }
}
