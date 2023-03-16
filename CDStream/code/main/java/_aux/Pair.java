package _aux;

public class Pair<X, Y> extends Object {
    public X _1;
    public Y _2;

    public Pair(X _1, Y _2) {
        this._1 = _1;
        this._2 = _2;
    }

    public String toString(){
        return "(" + _1.toString() + "," + _2.toString() + ")";
    }
    public void setFirst(X _1){this._1 = _1;}
    public void setSecond(Y _2){this._2 = _2;}
    public X getFirst(){return _1;}
    public Y getSecond(){return _2;}

}