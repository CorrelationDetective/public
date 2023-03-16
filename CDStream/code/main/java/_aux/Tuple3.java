package _aux;

import lombok.Getter;
import lombok.Setter;

public class Tuple3<X, Y, Z> extends Object {
    @Getter @Setter public X _1;
    @Getter @Setter public Y _2;
    @Getter @Setter public Z _3;

    public Tuple3(X _1, Y _2, Z _3) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }

}