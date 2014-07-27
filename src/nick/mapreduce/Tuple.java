package nick.mapreduce;

public class Tuple {
    private String key;
    private String value;
    
    public Tuple (String key, String value) {
        this.key = key;
        this.value = value;
    }
    
    public String fst() {
        return key;
    }
    
    public String snd() {
        return value;
    }
    
    @Override
    public boolean equals (Object o){
        try {
            Tuple tup = (Tuple) o;
            return fst().equals(tup.fst()) && snd().equals(tup.snd());
        } catch (ClassCastException e) {
            return false;
        }
    }
    
    @Override
    public String toString() {
        return "<"+key+", "+value+">";
    }
}
