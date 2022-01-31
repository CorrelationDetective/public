package hybrid;

import java.util.Random;

public class LinearRegressor {
    double meanX = 0;
    double meanY = 0;
    double varX = 0;
    double covXY = 0;
    int n=0;

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

    public double getA(){ return covXY/varX;}
    public double getB(){ return meanY - getA()*meanX;}
    public double predict(int x){
        return getA()*x + getB();
    }
}
