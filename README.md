# K-mean Clustering
k-mean clustering implementation in hadoop mapreduce

The program takes three input arguments:
1. input (the points, where each line describes the x and y coordinates of a point, comma-separated).
2. a text file that provides the range of the input points; where the first line in the file is the min and max x-coordinates, comma-separated, and the second line is the same but for the y-coordinates.
3. the integer k, which represents the number of clusters.

eg. `hadoop jar k-mean.jar input ranges.txt 3`

Note that the output file "output/centroids.txt" will be given in the following format:</br>
id,x-coordinate,y-coordinate,density</br>
the density represents the number of points in the cluster having this centroid.
