package _aux;

public class Pair<X, Y> extends Object {
    public X x;
    public Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public String toString(){
        return "(" + x.toString() + "," + y.toString() + ")";
    }
    public void setX(X x){this.x = x;}
    public void setY(Y y){this.y = y;}
}