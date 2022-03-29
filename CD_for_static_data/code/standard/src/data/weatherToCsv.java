package data;

import _aux.lib;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;

public class weatherToCsv {





    public static void main(String[] args){
        Writer out = null;
        {
            try {
                out = new FileWriter("weather-processed-4-2927-temp.csv");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        PrintWriter writer = new PrintWriter(out);

        String fpath ="../weather-processed.4.2927.temp";
        ArrayList<String> header = new ArrayList<>();

        double[][] data = readWeatherData.readWeather2(fpath, 2927, 8760, Measure.Attribute.temp, header);

        double[][] csv = readFMRI.transpose(data);

        writer.println(header.toString().substring(1, header.toString().length()-1));


        for(double[] row : csv){
            String r = Arrays.toString(row);
            writer.println(r.substring(1, r.length()-1));

        }
        writer.close();


    }






}
