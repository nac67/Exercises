package nick.mapreduce;

import java.util.List;

/**
 * To create a mapreduce job, the user must supply map and reduce instructions
 * in this format.
 * 
 * @author Nick Cheng
 */
public interface Instructions {
    
    public List<Tuple> map (Tuple input);
    
    public Tuple reduce (List<Tuple> input);
    
}
