package streaming;


import _aux.Pair;
import _aux.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CorrBoundTuple {
    //    Form: Pair<Tuple3<idx1, idx2, lower>, Tuple3<idx1, idx2, upper>>
    public double lower;
    public double upper;
    public int fractionCase;
    public int state;
    public double max2CorrLowerBound;
    public double centerOfBounds;
    public double criticalShrinkFactor;
    public double slack;
    public double shrunkUB;


    public int[] withinLHS_BDP1_LB;
    public int[] withinLHS_BDP2_LB;
    public double[][] withinLHS_corr_LB;

    public int[] withinLHS_BDP1_UB;
    public int[] withinLHS_BDP2_UB;
    public double[][] withinLHS_corr_UB;

    public int[] withinRHS_BDP1_LB;
    public int[] withinRHS_BDP2_LB;
    public double[][] withinRHS_corr_LB;

    public int[] withinRHS_BDP1_UB;
    public int[] withinRHS_BDP2_UB;
    public double[][] withinRHS_corr_UB;

    public int[] between_BDP1_LB;
    public int[] between_BDP2_LB;
    public double[][] between_corr_LB;

    public int[] between_BDP1_UB;
    public int[] between_BDP2_UB;
    public double[][] between_corr_UB;

    public int[] dom_withinLHS_BDP1;
    public int[] dom_withinLHS_BDP2;
    public double[][] dom_withinLHS_corr;

    public int[] dom_withinRHS_BDP1;
    public int[] dom_withinRHS_BDP2;
    public double[][] dom_withinRHS_corr;

    public int[] dom_between_BDP1;
    public int[] dom_between_BDP2;
    public double[][] dom_between_corr;

    public CorrBoundTuple(
            ArrayList<Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> withinLHSBounds,
            ArrayList<Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> withinRHSBounds,
            ArrayList<Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> betweenClusterBounds,
            double max2CorrLowerBound
    ) {
        this.fractionCase = 0;

//        Handle withinLHS bounds
        this.withinLHS_BDP1_LB = new int[withinLHSBounds.size()];
        this.withinLHS_BDP2_LB = new int[withinLHSBounds.size()];
        this.withinLHS_corr_LB = new double[withinLHSBounds.size()][];

        this.withinLHS_BDP1_UB = new int[withinLHSBounds.size()];
        this.withinLHS_BDP2_UB = new int[withinLHSBounds.size()];
        this.withinLHS_corr_UB = new double[withinLHSBounds.size()][];

        if (withinLHSBounds.size() > 0){
            IntStream.range(0, withinLHSBounds.size()).forEach(i -> {
                this.withinLHS_BDP1_LB[i] = withinLHSBounds.get(i).x.x;
                this.withinLHS_BDP2_LB[i] = withinLHSBounds.get(i).x.y;
                this.withinLHS_corr_LB[i] = withinLHSBounds.get(i).x.z;

                this.withinLHS_BDP1_UB[i] = withinLHSBounds.get(i).y.x;
                this.withinLHS_BDP2_UB[i] = withinLHSBounds.get(i).y.y;
                this.withinLHS_corr_UB[i] = withinLHSBounds.get(i).y.z;
            });
        }

//        Handle withinRHS bounds
        this.withinRHS_BDP1_LB = new int[withinRHSBounds.size()];
        this.withinRHS_BDP2_LB = new int[withinRHSBounds.size()];
        this.withinRHS_corr_LB = new double[withinRHSBounds.size()][];

        this.withinRHS_BDP1_UB = new int[withinRHSBounds.size()];
        this.withinRHS_BDP2_UB = new int[withinRHSBounds.size()];
        this.withinRHS_corr_UB = new double[withinRHSBounds.size()][];

        if (withinRHSBounds.size() > 0) {
            IntStream.range(0, withinRHSBounds.size()).forEach(i -> {
                this.withinRHS_BDP1_LB[i] = withinRHSBounds.get(i).x.x;
                this.withinRHS_BDP2_LB[i] = withinRHSBounds.get(i).x.y;
                this.withinRHS_corr_LB[i] = withinRHSBounds.get(i).x.z;

                this.withinRHS_BDP1_UB[i] = withinRHSBounds.get(i).y.x;
                this.withinRHS_BDP2_UB[i] = withinRHSBounds.get(i).y.y;
                this.withinRHS_corr_UB[i] = withinRHSBounds.get(i).y.z;
            });
        }

//        Handle between bounds
        this.between_BDP1_LB = new int[betweenClusterBounds.size()];
        this.between_BDP2_LB = new int[betweenClusterBounds.size()];
        this.between_corr_LB = new double[betweenClusterBounds.size()][];

        this.between_BDP1_UB = new int[betweenClusterBounds.size()];
        this.between_BDP2_UB = new int[betweenClusterBounds.size()];
        this.between_corr_UB = new double[betweenClusterBounds.size()][];

        if (betweenClusterBounds.size() > 0) {
            IntStream.range(0, betweenClusterBounds.size()).forEach(i -> {
                this.between_BDP1_LB[i] = betweenClusterBounds.get(i).x.x;
                this.between_BDP2_LB[i] = betweenClusterBounds.get(i).x.y;
                this.between_corr_LB[i] = betweenClusterBounds.get(i).x.z;

                this.between_BDP1_UB[i] = betweenClusterBounds.get(i).y.x;
                this.between_BDP2_UB[i] = betweenClusterBounds.get(i).y.y;
                this.between_corr_UB[i] = betweenClusterBounds.get(i).y.z;
            });
        }

        this.max2CorrLowerBound = max2CorrLowerBound;
    }

    public void setDomBounds(){
        if (state == 1){
            switch (fractionCase) {
                case 0: {
                    dom_withinLHS_BDP1 = withinLHS_BDP1_UB;
                    dom_withinLHS_BDP2 = withinLHS_BDP2_UB;
                    dom_withinLHS_corr = withinLHS_corr_UB;

                    dom_withinRHS_BDP1 = withinRHS_BDP1_UB;
                    dom_withinRHS_BDP2 = withinRHS_BDP2_UB;
                    dom_withinRHS_corr = withinRHS_corr_UB;

                    dom_between_BDP1 = between_BDP1_LB;
                    dom_between_BDP2 = between_BDP2_LB;
                    dom_between_corr = between_corr_LB;
                } break;
                case -1:
                case 1:
                case 2: {
                    dom_withinLHS_BDP1 = withinLHS_BDP1_LB;
                    dom_withinLHS_BDP2 = withinLHS_BDP2_LB;
                    dom_withinLHS_corr = withinLHS_corr_LB;

                    dom_withinRHS_BDP1 = withinRHS_BDP1_LB;
                    dom_withinRHS_BDP2 = withinRHS_BDP2_LB;
                    dom_withinRHS_corr = withinRHS_corr_LB;

                    dom_between_BDP1 = between_BDP1_LB;
                    dom_between_BDP2 = between_BDP2_LB;
                    dom_between_corr = between_corr_LB;
                }break;
            }
        } else if (state == -1){
            switch (fractionCase) {
                case -1:
                case 0:
                case 1: {
                    dom_withinLHS_BDP1 = withinLHS_BDP1_LB;
                    dom_withinLHS_BDP2 = withinLHS_BDP2_LB;
                    dom_withinLHS_corr = withinLHS_corr_LB;

                    dom_withinRHS_BDP1 = withinRHS_BDP1_LB;
                    dom_withinRHS_BDP2 = withinRHS_BDP2_LB;
                    dom_withinRHS_corr = withinRHS_corr_LB;

                    dom_between_BDP1 = between_BDP1_UB;
                    dom_between_BDP2 = between_BDP2_UB;
                    dom_between_corr = between_corr_UB;
                }break;
                case 2: {
                    dom_withinLHS_BDP1 = withinLHS_BDP1_UB;
                    dom_withinLHS_BDP2 = withinLHS_BDP2_UB;
                    dom_withinLHS_corr = withinLHS_corr_UB;

                    dom_withinRHS_BDP1 = withinRHS_BDP1_UB;
                    dom_withinRHS_BDP2 = withinRHS_BDP2_UB;
                    dom_withinRHS_corr = withinRHS_corr_UB;

                    dom_between_BDP1 = between_BDP1_UB;
                    dom_between_BDP2 = between_BDP2_UB;
                    dom_between_corr = between_corr_UB;
                }break;
            }
        }
    }

    public int setState(double corrThreshold, double minJump, double geometricMean, double shrinkFactor, double maxApproximationSize) {
        this.centerOfBounds = lower + ((upper - lower) / 2);
        this.slack = upper - centerOfBounds;
        this.shrunkUB = getShrunkUB(geometricMean, centerOfBounds, slack, shrinkFactor, maxApproximationSize);

        double jumpBasedThreshold = this.max2CorrLowerBound + minJump;
        double maxThreshold = Math.max(jumpBasedThreshold, corrThreshold);

        if (slack > 0 && maxThreshold <= 1) {
            this.criticalShrinkFactor = (maxThreshold - centerOfBounds) / slack;
        }else {
            this.criticalShrinkFactor = 10;
        }

        if ((lower < maxThreshold) && (shrunkUB >= maxThreshold)){
            this.state = 0; // is undecisive
        } else if (shrunkUB < maxThreshold) {
            this.state = -1; // does not comply to threshold
        } else if (lower >= maxThreshold){
            this.state = 1;
        } else {
            throw new IllegalStateException("undefined cluster combination state!");
        }
        return state;
    }


    public double getShrunkUB(double geometricMean, double centerOfBounds, double slack,
                              double shrinkFactor, double maxApproximationSize){
        if (geometricMean < maxApproximationSize){
            return centerOfBounds + (slack) * shrinkFactor;
        } else {
            return upper;
        }
    }
}
