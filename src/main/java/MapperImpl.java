import model.Centroid;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;


public class MapperImpl extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Centroid> {

    private final ArrayList<Centroid> centroids = new ArrayList<>();

    public void configure(JobConf job) {
        try {
            FileSystem fs = FileSystem.getLocal(job);
            InputStream inputStream = fs.open(new Path("output/centroids.txt"));
            Scanner scanner = new Scanner(inputStream);

            while (scanner.hasNextLine())
                centroids.add(Centroid.getFromRecord(scanner.nextLine(), ","));

        } catch (IOException ignored) {
        }

    }

    @Override
    public void map(LongWritable longWritable, Text text,
                    OutputCollector<IntWritable, Centroid> outputCollector, Reporter reporter) throws IOException {

        Centroid p = Centroid.getFromXY(text.toString(), ",");
        double minDistance;
        int closestPoint;

        assert !centroids.isEmpty();

        Centroid centroid = centroids.get(0);
        double distance = p.distanceFrom(centroid);
        minDistance = distance;
        closestPoint = centroid.getId();

        for (int i = 1; i < centroids.size(); i++) {
            centroid = centroids.get(i);
            distance = p.distanceFrom(centroid);
            if (distance < minDistance) {
                minDistance = distance;
                closestPoint = centroid.getId();
            }
        }

        outputCollector.collect(new IntWritable(closestPoint), p);
    }
}
