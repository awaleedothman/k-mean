package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable {

    protected final java.awt.Point point = new java.awt.Point();

    @SuppressWarnings("unused")
    public Point() {
    }

    public Point(double x, double y) {
        this.point.setLocation(x, y);
    }

    public static Point getFromXY(String line, String delimiter) {
        line = line.trim();
        String[] coordinates = line.split(delimiter);
        return new Point(Double.parseDouble(coordinates[0].trim()), Double.parseDouble(coordinates[1].trim()));
    }


    public Double getX() {
        return this.point.getX();
    }

    public Double getY() {
        return this.point.getY();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(point.getX());
        dataOutput.writeDouble(point.getY());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        point.setLocation(dataInput.readDouble(), dataInput.readDouble());
    }

    public double distanceFrom(Point p) {
        double x_2 = Math.pow(Math.abs(this.getX() - p.getX()), 2);
        double y_2 = Math.pow(Math.abs(this.getY() - p.getY()), 2);
        return Math.sqrt(x_2 + y_2);
    }

    @Override
    public String toString() {
        return point.getX() + "," + point.getY();
    }
}
