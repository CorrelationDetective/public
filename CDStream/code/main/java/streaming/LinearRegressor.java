package streaming;

import lombok.Getter;
import lombok.NonNull;

public class LinearRegressor {
    @Getter private double meanX = 0;
    @Getter private double meanY = 0;
    @Getter private double varX = 0;
    @Getter private double covXY = 0;
    @Getter private int n=0;

//    Y = aX + b or just Y = b
    private boolean hasX = true;

    public LinearRegressor(boolean hasX){
        this.hasX = hasX;
    }

    public LinearRegressor(){}

    public void update(int x, double y){
        n++;
        double dx = x - meanX;
        double dy = y - meanY;
        varX += (((n-1d)/n)*dx*dx - varX)/n;
        covXY += (((n-1d)/n)*dx*dy - covXY)/n;
        meanX += dx/n;
        meanY += dy/n;
    }

    public double getB(){ return meanY - getA()*meanX;}

    public double getA(){ if (varX == 0) return 0; else return covXY/(varX);}
    public double predict(int x){
        if (hasX){
            return getA()*x + getB();
        } else {
            return meanY;
        }
    }
}