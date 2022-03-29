package _aux;


public class Tuple3<X, Y, Z> extends Object {
    public X x;
    public Y y;
    public Z z;

    public Tuple3(X x, Y y, Z z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public void setX(X x){this.x = x;}
    public void setY(Y y){this.y = y;}
    public void setZ(Z z){this.z = z;}
}