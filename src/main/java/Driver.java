import model.Centroid;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class Driver {

    public static void main(String[] args) throws IOException {
        assert args.length == 3;

        final int MAX_ITER = 100, TOLERANCE = 2;

        final Path rangesPath = new Path(args[1]);
        final int K = Integer.parseInt(args[2]);
        final Path centroidsPath = new Path("output/centroids.txt");

        JobConf conf = new JobConf(Driver.class);
        conf.setJobName("k-mean");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Centroid.class);
        conf.set("mapred.textoutputformat.separator", ",");
        conf.setMapperClass(MapperImpl.class);
        conf.setCombinerClass(ReducerImpl.class);
        conf.setReducerClass(ReducerImpl.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path("tmp"));

        ArrayList<Integer> prev, curr;
        int iterNum = 0, res;

        FileSystem fs = FileSystem.getLocal(conf);

        prev = generateCentroids(centroidsPath, rangesPath, K);

        do {
            iterNum++;
            JobClient.runJob(conf);
            fs.copyFromLocalFile(true, true, new Path("tmp/part-00000"), centroidsPath);
            fs.delete(new Path("tmp"), true);
            curr = readCentroids(centroidsPath);
            res = compareCentroids(prev, curr);
            prev = curr;

        } while (res > TOLERANCE && iterNum < MAX_ITER);
    }


    private static ArrayList<Integer> generateCentroids(Path centroidsPath, Path rangesPath, int k)
            throws IOException {

        ArrayList<Integer> centroids = new ArrayList<>();
        double xMin, xMax, yMin, yMax;

        Scanner scanner = new Scanner(new File(rangesPath.toString()));
        String[] line = scanner.nextLine().split(",");
        xMin = Double.parseDouble(line[0].trim());
        xMax = Double.parseDouble(line[1].trim());
        line = scanner.nextLine().split(",");
        yMin = Double.parseDouble(line[0].trim());
        yMax = Double.parseDouble(line[1].trim());

        int lastSep = centroidsPath.toString().lastIndexOf("/");
        String dirName = centroidsPath.toString().substring(0, lastSep);
        File dir = new File(dirName);
        if (!dir.mkdirs()) {
            System.err.println("Error creating output directory. The directory \"output\" may already exist.");
            System.exit(-1);
        }
        String fileName = centroidsPath.toString().substring(lastSep + 1);
        File f = new File(dir, fileName);
        if (!f.createNewFile()) {
            System.err.println("centroids.txt already exists in the given output directory");
            System.exit(-1);
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(f));
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            double x = random.nextDouble() * (xMax - xMin) + xMin;
            double y = random.nextDouble() * (yMax - yMin) + yMin;
            writer.write(i + "," + x + "," + y + "," + 0 + "\n");
        }
        writer.close();
        return centroids;
    }

    private static ArrayList<Integer> readCentroids(Path centroidsPath) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(centroidsPath.toString()));
        ArrayList<Integer> centroids = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String[] line = scanner.nextLine().replaceAll("[^\\d.,]", "").split(",");
            int id = Integer.parseInt(line[0].trim());
            int density = Integer.parseInt(line[3].trim());
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
