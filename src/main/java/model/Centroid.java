package model;

import org.apache.hadoop.io.Writable;

import java.awt.Point;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements Writable {

    protected final Point point = new Point();

    private int memberNum;
    private Integer id = null;


    @SuppressWarnings("unused")
    public Centroid() {
    }

    public Centroid(double x, double y, int memberNum) {
        this.point.setLocation(x, y);
        this.memberNum = memberNum;
    }


    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getId() {
        return this.id;
    }

    public int getMemberNum() {
        return memberNum;
    }

    public static Centroid getFromXY(String line, String delimiter) {
        String[] coordinates = line.replaceAll("[^\\d.,]", "").split(delimiter);
        assert coordinates.length == 2;
        double x = Double.parseDouble(coordinates[0].trim());
        double y = Double.parseDouble(coordinates[1].trim());
        return new Centroid(x, y, 1);
    }

    public static Centroid getFromRecord(String line, String delimiter) {
        String[] tokens = line.replaceAll("[^\\d.,]", "").split(delimiter);
        assert tokens.length == 4;
        double x = Double.parseDouble(tokens[1].trim());
        double y = Double.parseDouble(tokens[2].trim());
        Centroid centroid = new Centroid(x, y, Integer.parseInt(tokens[3].trim()));
        centroid.setId(Integer.parseInt(tokens[0].trim()));
        return centroid;
    }

    public Double getX() {
        return this.point.getX();
    }

    public Double getY() {
        return this.point.getY();
    }

    public double distanceFrom(Centroid p) {
        double x_2 = Math.pow(Math.abs(this.getX() - p.getX()), 2);
        double y_2 = Math.pow(Math.abs(this.getY() - p.getY()), 2);
        return Math.sqrt(x_2 + y_2);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(point.getX());
        dataOutput.writeDouble(point.getY());
        dataOutput.writeInt(memberNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        point.setLocation(dataInput.readDouble(), dataInput.readDouble());
        memberNum = dataInput.readInt();
    }

    @Override
    public String toString() {
        return point.getX() + "," + point.getY() + "," + memberNum;
    }
}
