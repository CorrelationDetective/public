package data;

import _aux.Pair;
import _aux.lib;
import data.Measure.Attribute;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import java.io.*;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;


public class StreamDataReader {
    public static double[][] readBatchedCSV(String pathPrefix, int n, int m, ArrayList<Integer> blackList, int offset) {
        String delimiter = ",";
        try {
            ArrayList<double[]> in = new ArrayList<>();
            List<String> files = Files.list(new File(pathPrefix).toPath())
                    .map(x -> x.toString())
                    .sorted((String a, String b) -> {
                                if (a.length() != b.length()) {
                                    return Integer.compare(a.length(), b.length());
                                } else {
                                    return a.compareTo(b);
                                }
                            }
                    )
                    .limit(Math.floorDiv(n + blackList.size(), 100) + 2).collect(Collectors.toList());

            String path = files.remove(0);
            BufferedReader br = new BufferedReader(new FileReader(path));

            int idx = 0;

//            Read rows until required n_rows is obtained
            while (in.size() < n) {
//                Read until file has no lines anymore
                while (br.ready()) {
//                    Skip rows that are in blacklist
                    String[] line = br.readLine().split(delimiter);

                    if (!blackList.contains(idx)) {
                        int effLength = Math.min(line.length - offset, m);

                        double[] val = new double[effLength];
                        //  Skip nan stocks
                        for (int i = 0; i < effLength; i++) {
                            double cell = Double.parseDouble(line[i + offset]);
                            val[i] = cell;
                        }
                        in.add(val);
                    }
                    idx++;
                }
//                Get next file if last file is fully read through
                path = files.remove(0);
                if (path == null) {
                    System.out.println("Out of files before nrows was attained");
                    break;
                } else {
                    br = new BufferedReader(new FileReader(String.valueOf(path)));
                }
            }

            double[][] ret = new double[n][];
//            Trim off non-necessary padding
            for (int i = 0; i < Math.min(n, in.size()); i++)
                ret[i] = in.get(i);
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static HashMap<Integer, Double> getLambdas(String path, int offset) {
        String delimiter = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            HashMap<Integer, Double> lambdaMap = new HashMap<>();
            int i = 0;
            while(br.ready()) {
                String[] line = br.readLine().split(delimiter);
                int idx = offset > 0 ? (int) Double.parseDouble(line[0]): i;
                double lamb;
                if (line[offset].equals("inf")){
                    lamb = 0d;
                } else {
                    lamb = Double.parseDouble(line[offset]);
                }
                lambdaMap.put(idx, lamb);
                i++;
            }
            return lambdaMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static Pair<String[], double[][]> readColumnMajorCSV(String path, int maxN, int maxDim) {
        String delimiter = ",";

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));

//            Get Header
            String firstLine = br.readLine();
            String[] header = firstLine.split(delimiter);
            int n = header.length;
            int effN = Math.min(n, maxN);

//            Parse data
            ArrayList<double[]> rows = new ArrayList<>();
            int m = 0;
            while (br.ready() && m < maxDim) {
                String[] line = br.readLine().split(delimiter);
                double[] row = new double[effN];

//                Distribute values over columns
                for (int i = 0; i < effN; i++) {
                    if (line[i].equals("nan")) {
                        System.out.println("nan value");
                    }

                    row[i] = Double.parseDouble(line[i]);
                }
                rows.add(row);
                m++;
            }

//            Convert rows arraylist to array
            double[][] res = new double[m][n];
            for (int i = 0; i < rows.size(); i++) {
                res[i] = rows.get(i);
            }
//            Transpose result
            res = lib.transpose(res);

            return new Pair<>(header, res);
        } catch (Exception e) {
            System.out.println("File not found: " + path);
        }
        return null;
    }


    public static HashMap<String, int[]> getArrivalTimes(String path, int w) {
        String delimiter = ",";
        try {
            HashMap<String, int[]> result = new HashMap<>();

            BufferedReader br = new BufferedReader(new FileReader(path));

//            Parse data
            while (br.ready()) {
                String[] line = br.readLine().split(delimiter);
                String id = line[0];

                int[] times = IntStream.range(1, line.length)
                        .map(i -> Integer.parseInt(line[i]))
                        .toArray();

//                Add to result
                result.put(id, times);
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}