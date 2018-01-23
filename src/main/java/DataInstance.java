import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Pavlo_Suslykov on 9/20/2017.
 */
public interface DataInstance {
    List<Map> describe(String object);
    ArrayList query(String query);
    void load(String object, List<Map> items);
}
