package _aux;


public class ResultsTuple {
    public int[] points;
    public double lowerBound;
    public double upperBound;

    public ResultsTuple(int[] points, double lowerBound, double upperBound) {
        this.points = points;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

}
