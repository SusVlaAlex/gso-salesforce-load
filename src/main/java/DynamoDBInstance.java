import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Pavlo_Suslykov on 6/6/2017.
 */
public class DynamoDBInstance implements DataInstance {
    static AmazonDynamoDB dbClient;
    final Logger logger = LoggerFactory.getLogger(SalesForceInstance.class);

    public DynamoDBInstance() {
        dbClient = AmazonDynamoDBClientBuilder.standard()
                //.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000","us-west-2"))
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    @Override
    public List<Map> describe(String object) {
        return null;
    }

    @Override
    public ArrayList query(String query) {
        return null;
    }

    @Override
    public void load(String object, List<Map> items){
        this.logger.info("Loading data into DynamoDB table {}.", object);
        for(Map<String, String> item: items){
            //Converting values in item to type AttributeValue that is accepted by DynamoDB
            Map<String, AttributeValue> dynamo_item = new HashMap<String, AttributeValue>();
            for(String key_value : item.keySet()){
                String value = item.get(key_value);
                if(value!=null && value!=""){
                    dynamo_item.put(key_value,new AttributeValue().withS(value));
                }
            }
            //Loading item into table in DynamoDB
            PutItemRequest putItemRequest = new PutItemRequest(object, dynamo_item);
            try {
                PutItemResult putItemResult = dbClient.putItem(putItemRequest);

            } catch (AmazonServiceException ase) {
                this.logger.error("Could not complete operation");
                this.logger.error("Error Message:\t{}", ase.getMessage());
                this.logger.error("HTTP Status:\t{}", ase.getStatusCode());
                this.logger.error("AWS Error Code:\t{}", ase.getErrorCode());
                this.logger.error("Error Type:\t{}", ase.getErrorType());
                this.logger.error("Request ID:\t{}", ase.getRequestId());
                this.logger.error("The Item with error:\n {}", item.toString());
                throw new RuntimeException();
            } catch (AmazonClientException ace) {
                this.logger.error("Internal error occured communicating with DynamoDB");
                this.logger.error("Error Message:\t{}", ace.getMessage());
                this.logger.error("The Item with error:\n {}", item.toString());
                throw new RuntimeException();
            }
            catch (Exception e) {
                this.logger.error(e.toString());
                this.logger.error("The Item with error:\n {}", item.toString());
                throw new RuntimeException();
            }
        }
        this.logger.info("The {} records were loaded.", items.size());
    }
}
