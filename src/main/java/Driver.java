import model.Centroid;
import model.Point;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Driver {


    public static void main(String[] args) throws IOException {
        final int MAX_ITER = 100, TOLERANCE = 2;
        final Path centroidsPath = new Path(args[2]);

        assert args.length == 3;

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

        ArrayList<Integer> prev, curr;
        int iterNum = 0, res;

        FileSystem fs = FileSystem.getLocal(conf);

        prev = readCentroids(centroidsPath);

        do {
            iterNum++;
            JobClient.runJob(conf);
            fs.copyFromLocalFile(true, true, new Path(args[1] + "/part-00000"), centroidsPath);
            fs.delete(new Path(args[1]), true);
            curr = readCentroids(centroidsPath);
            res = compareCentroids(prev, curr);
            prev = curr;

        } while (res > TOLERANCE && iterNum < MAX_ITER);
    }


    private static ArrayList<Integer> readCentroids(Path centroidsPath) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(centroidsPath.toString()));
        ArrayList<Integer> centroids = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String[] line = scanner.nextLine().replaceAll("[^\\d.,]", "").split(",");
            int id = Integer.parseInt(line[0]) - 1;
            int density = Integer.parseInt(line[3]);
            centroids.add(id, density);
        }
        return centroids;
    }

    private static int compareCentroids(ArrayList<Integer> prev, ArrayList<Integer> curr) {
        int diff = 0;

        for (int i = 0; i < prev.size(); i++)
            diff += Math.abs(prev.get(i) - curr.get(i));

        return diff / 2;
    }
}
