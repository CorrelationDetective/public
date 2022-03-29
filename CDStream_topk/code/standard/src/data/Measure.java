package data;

public class Measure implements Comparable<Measure> {
    final String id;
    final float lat, lon, elevation;
    final long timestamp;
    final int windSpeed, visibility, temp, dew, slp;

    public enum Attribute {
        windSpeed, visibility, temp, dew, slp
    }

    public Measure(String id, long timestamp, float lat, float lon, float elevation, Integer windSpeed, Integer visibility, Integer temp, Integer dew, Integer slp) {
        this.id=id;
        this.timestamp=timestamp;
        this.lat=lat;
        this.lon=lon;
        this.elevation=elevation;
        this.windSpeed=windSpeed;
        this.visibility=visibility;
        this.temp=temp;
        this.dew=dew;
        this.slp=slp;
    }
    public Measure(long timestamp, Measure m) {
        this.id=m.id;
        this.timestamp=timestamp;
        this.lat=m.lat;
        this.lon=m.lon;
        this.elevation=m.elevation;
        this.windSpeed=m.windSpeed;
        this.visibility=m.visibility;
        this.temp=m.temp;
        this.dew=m.dew;
        this.slp=m.slp;
    }

    public double getAttribute(Attribute attrToGet) {
        switch(attrToGet) {
            case windSpeed:
                return windSpeed;
            case visibility:
                return visibility;
            case temp:
                return temp;
            case dew:
                return dew;
            case slp:
                return slp;
        }
        return -1;
    }
    @Override
    public int compareTo(Measure o) {
        double timeDiff = (this.timestamp - o.timestamp);
        if (timeDiff > 0)
            return 1;
        else if (timeDiff < 0)
            return -1;
        else
            return this.id.compareTo(o.id);
    }
    public String toString() {
        return id + "," + timestamp + ",(" + lat + "," + lon + ",H" + elevation + "),["+ windSpeed + "," + visibility + "," + temp
                +"," + dew + "," + slp + "]";
    }
    public String toString2() {
        return timestamp + "," + lat + "," + lon + "," + elevation + ","+ windSpeed + "," + visibility + "," + temp
                +"," + dew + "," + slp;
    }
    public static Measure fromString2(String id, String s) {
        String[] s2 = s.split(",");
        Measure m = new Measure(id,
                Long.parseLong(s2[0]),
                Float.parseFloat(s2[1]),
                Float.parseFloat(s2[2]),
                Float.parseFloat(s2[3]),
                Integer.parseInt(s2[4]),
                Integer.parseInt(s2[5]),
                Integer.parseInt(s2[6]),
                Integer.parseInt(s2[7]),
                Integer.parseInt(s2[8]));
        return m;
    }
}