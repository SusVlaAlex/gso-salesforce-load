import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;

/**
 * Created by Pavlo_Suslykov on 9/18/2017.
 */
public class TestDynamoDB {
    public static void main(String[] args){
        System.out.println("Testing Dynamo DB functionality");
        testInsert();
    }

    public static void testInsert(){
        HashMap<String, String> item =new HashMap();
        item.put("Id","AAAAAAAA");
        item.put("Name","Test Record");
        item.put("IntegerValue", "13");

        DynamoDBInstance db = new DynamoDBInstance();
        //b.load("Case", item);
    }
}
