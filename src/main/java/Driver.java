import model.Centroid;
import model.Point;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;

public class Driver {

    private static int K = 3, MAX_ITER = 100;
    private static double EPSILON = 0.01;


    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Driver.class);
        conf.setJobName("k-mean");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Centroid.class);
        conf.setMapOutputValueClass(Point.class);
        conf.set("mapred.textoutputformat.separator", ",");
        conf.setMapperClass(MapperImpl.class);
        conf.setReducerClass(ReducerImpl.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        ArrayList<Point> prev = new ArrayList<>();
        ArrayList<Point> curr = new ArrayList<>();
        int iterNum = 0;
        Path centroidsPath = new Path("centroids.txt");
        FileSystem fs = FileSystem.getLocal(conf);

        prev = generateCentroids();
        writeCentroids(prev, centroidsPath);

        do {
            iterNum++;
            JobClient.runJob(conf);
            //TODO: update prev and curr
            fs.copyFromLocalFile(true, true, new Path(args[1] + "/part-00000"), centroidsPath);
            fs.delete(new Path(args[1]), true);
        } while (needsMore(prev, curr) && iterNum < MAX_ITER);
    }

    private static ArrayList<Point> generateCentroids() {
        //TODO
        return new ArrayList<>();
    }

    private static void writeCentroids(ArrayList<Point> points, Path centroidsPath) {
        //TODO
    }

    private static boolean needsMore(ArrayList<Point> prev, ArrayList<Point> curr) {
        //TODO
        return false;
    }
}
