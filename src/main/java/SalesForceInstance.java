import com.sforce.soap.partner.*;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class SalesForceInstance implements DataInstance{
    private final Logger logger = LoggerFactory.getLogger(SalesForceInstance.class);
    private PartnerConnection connection;

    SalesForceInstance(){
        /* Getting credentials from conf file in home directory */
        String conf = System.getProperty("user.home") + "\\.salesforce\\credentials";
        Properties prop = new Properties();
        try {
            this.logger.info("Reading properties file {}.", conf);
            FileInputStream in = new FileInputStream(conf);
            prop.load(in);
            in.close();
        } catch (FileNotFoundException e) {
            this.logger.error("The property file {} doesn't exists or permissions denied.", conf);
            this.logger.error(e.toString());
            throw new RuntimeException();
        } catch (IOException e) {
            this.logger.error(e.toString());
            throw new RuntimeException();
        }

        this.logger.info("Establishing connection to the SalesForce");
        ConnectorConfig config = new ConnectorConfig();
        config.setUsername(prop.getProperty("username"));
        config.setPassword(prop.getProperty("password")+prop.getProperty("token"));

        try {
            connection = Connector.newConnection(config);
            this.logger.debug("Connection established successfully");
        }
        catch (ConnectionException ce){
            this.logger.error(ce.getMessage());
            ce.printStackTrace();
        }
    }

    @Override
    public List<Map> describe(String object) {
        List<Map> result = new ArrayList<>();
        try {
            this.logger.debug("Getting list of available fields in object {}.",object);
            DescribeSObjectResult describeResult = this.connection.describeSObject(object);
            for(Field field: describeResult.getFields()){
                HashMap<String, String> itm = new HashMap<>();
                itm.put("name", String.valueOf(field.getName()));
                String sourcetype = String.valueOf(field.getSoapType());
                String datatype;
                switch(sourcetype){
                    case "ID": datatype = "string"; break;
                    default: datatype = "string";
                }
                itm.put("datatype", datatype);
                result.add(itm);
                this.logger.debug(itm.toString());
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public ArrayList query(String objectName) {
        String query = this.generateSOQLQuery(objectName);
        List<Map> fields = describe(objectName);
        ArrayList resultList = new ArrayList();
        SObject record=null;
        int recordsCount=0;
        try {
            this.logger.info("Running SOQL query: \n{}",query);
            QueryResult queryResult = connection.query(query);
            this.logger.info("Query result retuned {} records.", queryResult.getSize());
            this.logger.info("Fetching records from result set.");
            boolean done = false;
            while(!done) {
                for (recordsCount = 0; recordsCount < queryResult.getSize(); recordsCount++) {
                    record = queryResult.getRecords()[recordsCount];
                    Map<String, String> itm = new HashMap<>();
                    for (Map field : fields) {
                        String fieldName = (String) field.get("name");
                        if(record.getField(fieldName) != null){
                            itm.put(fieldName, record.getField(fieldName).toString());
                        } else {
                            itm.put(fieldName, "");
                        }
                    }
                    resultList.add(itm);
                }
                if(queryResult.isDone()){
                    done = true;
                } else {
                    queryResult = connection.queryMore(queryResult.getQueryLocator());
                }
            }
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            this.logger.error("Error during result processing.");
            if(record==null){
                this.logger.error("Fetched record is empty");
            } else{
                String errorRecord = record.toString();
                this.logger.error("Fetched record: {}",errorRecord);
            }
            this.logger.error(e.toString());
            throw new RuntimeException();
        }
        this.logger.info("Total records fetched: {}", recordsCount);
        return resultList;
    }

    @Override
    public void load(String object, List<Map> items) {}

    /**
     * Generates SOQL query that gets all available fields in SalesForce object.
     *
     * @return String that can be used to query object in SalesForce
     */
    private String generateSOQLQuery(String objectName){
        StringBuilder soql= new StringBuilder("select ");
        for(Map field: this.describe(objectName)){
            soql.append((String) field.get("name")).append(", ");
        }
        soql = new StringBuilder(soql.substring(0, soql.length() - 2) + " from " + objectName);
        return soql.toString();
    }
}
