package nick.mapreduce;

import java.util.List;

public interface Instructions {
    
    public List<Tuple> map (Tuple input);
    
    public Tuple reduce (List<Tuple> input);
    
}
