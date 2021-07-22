import model.Centroid;
import model.Point;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ReducerImpl extends MapReduceBase implements Reducer<IntWritable, Point, IntWritable, Centroid> {

    @Override
    public void reduce(IntWritable intWritable, Iterator<Point> iterator,
                       OutputCollector<IntWritable, Centroid> outputCollector, Reporter reporter) throws IOException {
        int count = 0;
        double sumX = 0, sumY = 0;

        while (iterator.hasNext()) {
            Point p = iterator.next();
            sumX += p.getX();
            sumY += p.getY();
            count++;
        }

        outputCollector.collect(intWritable, new Centroid(sumX / count, sumY / count, count));
    }
}
