package _aux;

public class Key {

    public final int x;
    public final int y;

    public Key(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public String toString(){
        return "<" + x + "," + y + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Key)) return false;
        Key key = (Key) o;
        return x == key.x && y == key.y;
    }

    @Override
    public int hashCode() {
        int result = x;
        result = 31 * result + y;
        return result;
    }

}
