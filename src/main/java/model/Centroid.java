package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid extends Point{

    private int memberNum;
    private Integer id = null;


    public Centroid(double x, double y, int memberNum) {
        super(x, y);
        this.memberNum = memberNum;
    }

    @SuppressWarnings("unused")
    public Centroid() {
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
        return super.toString() + "," + memberNum;
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
}
