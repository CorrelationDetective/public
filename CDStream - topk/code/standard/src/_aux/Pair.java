package _aux;

import streaming.TimeSeries;

public class Pair<X, Y> extends Object {
    public X x;
    public Y y;
    public Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> mirror;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
        this.mirror = null;
    }

    public String toString(){
        return "(" + x.toString() + "," + y.toString() + ")";
    }
    public void setX(X x){this.x = x;}
    public void setY(Y y){this.y = y;}

    public Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>
    getMirror(TimeSeries[] timeSeries, Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> pair){
        if (this.mirror != null){
            return this.mirror;
        } else{
            this.mirror = new Pair<>(new Tuple3<>(pair.x.y, pair.x.x, timeSeries[pair.x.y].pairwiseCorrelations),
                    new Tuple3<>(pair.y.y, pair.y.x, timeSeries[pair.y.y].pairwiseCorrelations));
            return mirror;
        }
    }
}
