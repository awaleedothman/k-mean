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

    private static final String CENTROIDS_DIR = "output";
    private static final String CENTROIDS_FILE = "centroids.txt";

    public static void main(String[] args) throws IOException {
        assert args.length == 3;

        final int MAX_ITER = 100, TOLERANCE = 2;

        final Path rangesPath = new Path(args[1]);
        final int K = Integer.parseInt(args[2]);
        final Path centroidsPath = new Path(CENTROIDS_DIR + "/" + CENTROIDS_FILE);

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

        prev = generateCentroids(rangesPath, K);

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


    private static ArrayList<Integer> generateCentroids(Path rangesPath, int k)
            throws IOException {

        ArrayList<Integer> centroids = new ArrayList<>();
        Scanner scanner = new Scanner(new File(rangesPath.toString()));

        double[] xRange = readRange(scanner.nextLine());
        double[] yRange = readRange(scanner.nextLine());
        double xMin = xRange[0], xMax = xRange[1];
        double yMin = yRange[0], yMax = yRange[1];

        File f = createCentroidsFile();
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

    private static File createCentroidsFile() throws IOException {
        File dir = new File(CENTROIDS_DIR);
        if (!dir.mkdirs()) {
            System.err.println("Error creating output directory. The directory may already exist.");
            System.exit(-1);
        }

        File f = new File(dir, CENTROIDS_FILE);
        if (!f.createNewFile()) {
            System.err.println("Error creating output file inside directory.");
            System.exit(-1);
        }

        return f;
    }

    private static double[] readRange(String line) {
        double[] range = new double[2];
        String[] tokens = line.replaceAll("[^\\d.,]", "").split(",");
        range[0] = Double.parseDouble(tokens[0].trim());
        range[1] = Double.parseDouble(tokens[1].trim());
        return range;
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
