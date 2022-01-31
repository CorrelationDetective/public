package data;

import java.io.*;
import java.util.*;

import _aux.lib;
import _aux.lib.*;

import static _aux.lib.znorm;

public class PrepareStocks {
    static boolean binary=false;
    static ArrayList<String> allStockLabels = new ArrayList<>();
    static boolean checkForZeros=true;





    public static HashMap<String, double[][]> readAllStocks(int minAcceptedSize, int maxNumberOfTimeSeries, int randomseed, boolean breakLargeFiles) {
        int zeros=0;
        String path = "/Users/o.papapetrou/OneDrive - TU Eindhoven/datasets/stocks/";
        try {
            File f = new File(path);
            if (!f.exists()) {
                f = new File("/home/odysseasp/stocks/allData/all/");
                path="/home/odysseasp/stocks/allData/all/";
            }
            HashMap<String, ArrayList<String>> filenames = new HashMap<>();
            for (File ff : f.listFiles()) {
                if (!ff.getName().endsWith(".txt") || !ff.getName().startsWith("201801"))
                    continue;
                String filename = ff.getName();
                String filenameWithoutDate = filename.substring(filename.indexOf('_') + 1);
                ArrayList<String> l = new ArrayList<>();
                for (int year : new int[]{2018, 2019, 2020})
                    for (String mon : new String[]{"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"})
                        l.add(year + mon + "_" + filenameWithoutDate);
                filenames.put(ff.getName(), l);
            }

            HashMap<String, ArrayList<Double>[]> resultsBeforeInterpolation = new HashMap<>();

            int totalRead=0;
            for (Map.Entry<String, ArrayList<String>> e : filenames.entrySet()) {
                System.err.println(totalRead++ + "."  + e.getKey());
                ArrayList<Double>[] alsGlobal = new ArrayList[6];
                for (int i = 0; i < 6; i++) alsGlobal[i] = new ArrayList<>(10000);
                for (String filename : e.getValue()) {
                    File ff = new File(path + filename);
                    if (!ff.exists()) continue;

                    BufferedReader br = new BufferedReader(new FileReader(ff));
                    ArrayList<Double>[] alsLocal = new ArrayList[6];
                    for (int i = 0; i < 6; i++) alsLocal[i] = new ArrayList<>(10000);
                    String prevDate = "";
                    while (br.ready()) {
                        String line = br.readLine().toLowerCase();
//						if (line.startsWith("<!doctype html") || line.startsWith("curl"))
//							break;
//						if (line.startsWith("date")) continue;
                        if (line.isEmpty()) continue;
                        int pos1 = 0;
                        int pos2 = 12;
                        int cnt = 0;
                        pos1 = line.indexOf(',', pos2);
                        pos2 = line.indexOf(',', pos1 + 1);
                        if (pos1<=0 || pos2<=0) {
                            System.err.println(filename + "[" + line + "]");
                        }
                        String date = line.substring(0, pos1);
                        String dateJustDay = date.substring(0,10);
                        if (!dateJustDay.equals(prevDate)) {
                            if (alsLocal[0].size()>0) {
                                // add avg to alsGlobal and reset alsLocal
                                double[] avgs = new double[alsLocal.length];
                                for (int i=0;i<alsLocal.length;i++) {
                                    for (double d: alsLocal[i]) {
                                        avgs[i]+=d;
                                    }
                                    avgs[i]/=alsLocal[i].size();
                                }
                                avgs[0] = (double) new java.text.SimpleDateFormat("MM/dd/yyyy").parse(prevDate).getTime() / 1000;
                                for (int i=0;i<alsLocal.length;i++)
                                    alsGlobal[i].add(avgs[i]);
                                for (int i=0;i<alsLocal.length;i++) alsLocal[i].clear();
                            }
                            prevDate=dateJustDay;
                        }
                        long epoch = 0;
                        try {
                            epoch = new java.text.SimpleDateFormat("MM/dd/yyyy,HH:mm:ss").parse(date).getTime() / 1000;
                        } catch (Exception eee) {
                            epoch = new java.text.SimpleDateFormat("MM/dd/yyyy,HH:mm").parse(date).getTime() / 1000;
                        }
                        alsLocal[0].add((double) epoch);
                        pos1 = 0;
                        pos2 = 13;
                        while (pos2 > -1) {
                            pos1 = line.indexOf(',', pos2);
                            pos2 = line.indexOf(',', pos1 + 1);
                            if (pos2 < 0)
                                pos2 = line.length();
                            String substr = line.substring(pos1 + 1, pos2);
                            double val = Double.parseDouble(substr);
                            alsLocal[1+cnt++].add(val);
                            cnt %= 5;
                            if (pos2 == line.length()) pos2 = -1;
                        }
                    }
                    br.close();
                }
                if (alsGlobal[0].size()<minAcceptedSize) continue;

//				if (als[0].size()<2*minAcceptedSize) continue;
//				// now subsample
//				ArrayList<Double>[] alsSample = new ArrayList[6];
//				float stepSize = Math.max(1, als[0].size()/(minAcceptedSize*2f));
//				for (int i=0;i<alsSample.length;i++) {
//					alsSample[i] = new ArrayList<>(minAcceptedSize*2);
//					for (int cnt=0;cnt<minAcceptedSize*2;cnt++) {
//						alsSample[i].add(als[i].get((int)(stepSize*cnt)));
//					}
//				}
//				resultsBeforeInterpolation.put(e.getKey(), alsSample);
                resultsBeforeInterpolation.put(e.getKey(), alsGlobal);
                if (resultsBeforeInterpolation.size()>2*maxNumberOfTimeSeries) break;
            }

            double minVal = Long.MAX_VALUE;
            double maxVal = -100000;
            // now do some interpolation
            double ts1=0,ts2=0;
            for (ArrayList<Double>[] stock : resultsBeforeInterpolation.values()) {
                ts1 += stock[0].get(0)/resultsBeforeInterpolation.values().size();
                ts2 += stock[0].get(stock[0].size()-1)/resultsBeforeInterpolation.values().size();
            }
            double dur = ts2-ts1;
            minVal=ts1 + dur/10;
            maxVal=ts2 - dur/10;
            int len = minAcceptedSize;
            int step = (int) ((maxVal - minVal) / (len + 10));
            HashMap<String, double[][]> resultsAfterInterpolation = new HashMap<>();

            for (Map.Entry<String, ArrayList<Double>[]> e : resultsBeforeInterpolation.entrySet()) {
                ArrayList<Double>[] values = e.getValue();
                double[] x = new double[values[0].size()];
                for (int cnt = 0; cnt < values[0].size(); cnt++) x[cnt] = values[0].get(cnt);

                boolean interrupted = false;
                double[] timepoints = new double[len];
                ArrayList<double[]> listOfAnswers = new ArrayList<>();
                for (int attribute = 1; attribute <= 5; attribute++) {
                    double[] vals = new double[len];
                    if (interrupted) break;
                    double[] y = new double[values[attribute].size()];
                    for (int cnt = 0; cnt < values[attribute].size(); cnt++) y[cnt] = values[attribute].get(cnt);
                    Interpolator i = new Interpolator(x, y);
                    for (int j = 0; j < len; j++) {
                        double posX = minVal + step + j * step;
                        double val = i.interpolate(posX, step);
                        if (val >= 0) {// worked
                            timepoints[j] = posX;
                            vals[j] = val;
                        } else {
                            interrupted = true;
                            break;
                        }
                    }
                    if (!interrupted) listOfAnswers.add(vals);
                }
                if (!interrupted) {
                    double[][] resultsThisStock = new double[6][len];
                    resultsThisStock[0] = timepoints;
                    resultsThisStock[1] = listOfAnswers.get(0);
                    resultsThisStock[2] = listOfAnswers.get(1);
                    resultsThisStock[3] = listOfAnswers.get(2);
                    resultsThisStock[4] = listOfAnswers.get(3);
                    resultsThisStock[5] = listOfAnswers.get(4);
                    znorm(resultsThisStock[1]);
                    znorm(resultsThisStock[2]);
                    znorm(resultsThisStock[3]);
                    znorm(resultsThisStock[4]);
                    znorm(resultsThisStock[5]);
                    resultsAfterInterpolation.put(e.getKey(), resultsThisStock);
                }
            }
            return resultsAfterInterpolation;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        int rounds=200;
        int shares=5000;
        System.err.println("Usage: java data.prepareStocks numberOfRounds numberOfShares");
        if (args.length>0) {
            rounds = Integer.parseInt(args[0]);
            shares = Integer.parseInt(args[1]);
        }
        HashMap<String, double[][]> stocks = readAllStocks(rounds, shares, 0, false);
        System.err.println("All stocks read!:" + stocks.size() + " stocks of rounds: " + rounds);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter("stocks-processed." + rounds + "." + stocks.size()));

            bw.write("" + stocks.size() + "\n");
            bw.write("" + 6 + "\n");
            bw.write("" + rounds + "\n");
            for (Map.Entry<String, double[][]> e : stocks.entrySet()) {
                bw.write("" + e.getKey() + "\n");
                double[][] array = e.getValue();
                for (int i = 0; i < array.length; i++) {
                    for (int j = 0; j < array[i].length; j++) {
                        bw.write("" + (float) array[i][j] + " ");
                    }
                    bw.write("\n");
                }
            }
            bw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // now try to read back
        HashMap<String, double[][]> stocksNew = getStocks("stocks-processed." + rounds + "." + stocks.size());
        System.err.println(stocks.size() + " " + stocksNew.size());
    }

    public static HashMap<String,double[][]> getStocks(String s) {
        HashMap<String, double[][]> stocks = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(s));
            int numberOfStocks = Integer.parseInt(br.readLine());
            int sizeOfArray = Integer.parseInt(br.readLine());
            int len = Integer.parseInt(br.readLine());

            String stockName = "";
            for (int i = 0; i < numberOfStocks; i++) {
                stockName = br.readLine();
                double[][] myDoubles = new double[6][len];
                for (int j = 0; j < sizeOfArray; j++) {
                    String line = br.readLine();
                    String[] chars = line.split(" ");
                    for (int k = 0; k < len; k++) {
                        myDoubles[j][k] = Double.parseDouble(chars[k]);
                    }
                }
                stocks.put(stockName, myDoubles);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stocks;
    }
}

class Interpolator {
    double[] x;
    double[] y;
    public Interpolator(double[] x, double[] y) {
        this.x = x;
        this.y = y;
    }
    public double interpolate(double pos, double maxDistance) {
        int idx = Arrays.binarySearch(this.x, pos);
        if (idx>=0) { // then i found it
            return this.y[idx];
        } else {
            int insertionPoint = -1-idx;
            int pointBefore = insertionPoint-1;
            int pointAfter  = insertionPoint;
            if (pointBefore>=0 && pointAfter<=y.length-1) {
//				if (pos-this.x[pointBefore]>maxDistance|| this.x[pointAfter]-pos>maxDistance)
//					return -1;
                return (this.y[pointBefore]+this.y[pointAfter])/2d;
            } else if (pointBefore<0 && pointAfter<=y.length-1) {
//				if (this.x[pointAfter]-pos>maxDistance) return -1;
                return (this.y[pointAfter]);
            } else if (pointAfter==y.length) {
//				if (pos-this.x[pointBefore]>maxDistance) return -1;
                return (this.y[pointBefore]);
            }
        }
        return -1;
    }
}
