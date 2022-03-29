package data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class readFMRI {


    public static double[][] transpose(double[][] matrix) {
        double[][] t = new double[matrix[0].length][matrix.length];
        for (int i=0;i<matrix.length;i++)
            for (int j=0;j<matrix[0].length;j++)
                t[j][i]=matrix[i][j];
        return t;
    }

    public static double[][] readfmri(String path, ArrayList<String> header) {
        String delimiter = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            String firstLine = br.readLine();
            if (firstLine.contains(";")) delimiter=";";
            String[] h = firstLine.split(delimiter);
            for (String ss:h) header.add(ss);
            int numFields=h.length;
            ArrayList<double[]> in = new ArrayList<>();
            while(br.ready()) {
                String[] line = br.readLine().split(delimiter);
                double[] val = new double[numFields];
                for (int i=0;i<numFields;i++)
                    val[i] = Double.parseDouble(line[i]);
                in.add(val);
            }
            double[][] ret = new double[in.size()][];
            for (int i=0;i<in.size();i++)
                ret[i]=in.get(i);
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
