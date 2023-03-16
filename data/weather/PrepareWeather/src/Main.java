import java.io.IOException;

// Add the following jars in the classpath:
// Jama  https://math.nist.gov/javanumerics/jama/#Package
// org.apache.ant: ant-1.8.2.jar, https://mvnrepository.com/artifact/org.apache.ant/ant/1.8.2

public class Main {

    public static void main(String[] args) throws IOException {
        preprocessWeatherData.main(args);
        // the output file is written in
        //    BufferedWriter bw = new BufferedWriter(new FileWriter("weather-processed." + readingsPerDay + "." + notNullItems+"."+attributeToExtract));
        // read it with readWeatherData
    }
}
