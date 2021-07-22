import model.Centroid;
import model.Point;
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


public class MapperImpl extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Point> {

    private final ArrayList<Centroid> centroids = new ArrayList<>();

    public void configure(JobConf job) {
        try {
            FileSystem fs = FileSystem.getLocal(job);
            InputStream inputStream = fs.open(new Path("centroids.txt"));
            Scanner scanner = new Scanner(inputStream);
            while (scanner.hasNextLine()) {
                String[] line = scanner.nextLine().replaceAll("[^\\d.,]", "").split(",");
                assert line.length == 4;
                double x = Double.parseDouble(line[1].trim());
                double y = Double.parseDouble(line[2].trim());
                Centroid centroid = new Centroid(x, y, Integer.parseInt(line[3].trim()));
                centroid.setId(Integer.parseInt(line[0].trim()));
                centroids.add(centroid);
            }
        } catch (IOException ignored) {
        }

    }

    @Override
    public void map(LongWritable longWritable, Text text,
                    OutputCollector<IntWritable, Point> outputCollector, Reporter reporter) throws IOException {

        Point p = Point.getFromXY(text.toString(), ",");
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
