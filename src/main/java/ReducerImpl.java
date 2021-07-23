import model.Centroid;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ReducerImpl extends MapReduceBase implements Reducer<IntWritable, Centroid, IntWritable, Centroid> {

    @Override
    public void reduce(IntWritable intWritable, Iterator<Centroid> iterator,
                       OutputCollector<IntWritable, Centroid> outputCollector, Reporter reporter) throws IOException {
        int count = 0;
        double sumX = 0, sumY = 0;

        while (iterator.hasNext()) {
            Centroid p = iterator.next();
            int density = p.getMemberNum();

            sumX += p.getX() * density;
            sumY += p.getY() * density;
            count += density;
        }

        outputCollector.collect(intWritable, new Centroid(sumX / count, sumY / count, count));
    }
}
