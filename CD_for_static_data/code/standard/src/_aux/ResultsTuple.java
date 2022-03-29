package _aux;

import java.util.ArrayList;

public class ResultsTuple {
    int[] points;
    double lowerBound;
    double upperBound;
    String explanation;

    public ResultsTuple(int[] points, double lowerBound, double upperBound, String explanation) {
        this.points = points;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.explanation = explanation;
    }


    public String toString(ArrayList<String> stockNames) {
        return (stockNames.get(points[0]) + ", (" + stockNames.get(points[1]) + ", " + stockNames.get(points[2]) + ")" +
                "  decided with bounds [" + lowerBound + "," + upperBound + "] and explanation:" + explanation);
    }
}
