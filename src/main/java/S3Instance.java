import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class S3Instance implements DataInstance{
    private AmazonS3 s3;
    private final Logger logger = LoggerFactory.getLogger(SalesForceInstance.class);

    public S3Instance(){
        try{
            s3 = AmazonS3Client.builder().withRegion("us-east-1").build();
        } catch (AmazonServiceException e){
            this.logger.error(e.toString());
            throw new RuntimeException();
        }
    }

    public void putFile(String file, String bucketName, String s3ObjectKey){
        try{
            File f = new File(file);
            //this.s3.putObject(bucketName, s3ObjectKey, f);
        } catch (AmazonServiceException e) {
            this.logger.error(e.toString());
            throw new RuntimeException();
        }
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
    public void load(String object, List<Map> items) {
        //Saving data into temporary file on local machine.
        this.logger.info("Creating JSON file in temporary folder");
        String file = createTempJSON(items);
        File f = new File(file);

        //Loading temporary file into S3
        String s3bucket = "psuslykov-salesforce-us-east-1";
        String s3target = object+"/"+f.getName();
        this.logger.info("Loading temporary file into S3.");
        this.logger.info("Target S3 bucket:", s3bucket);
        this.logger.info("Target S3 file:", s3target);
        this.s3.putObject(s3bucket, s3target, f);

    }

    protected String createTempJSON(List<Map> items){
        String tmpdir = System.getProperty("java.io.tmpdir");
        String outputFile = tmpdir.concat(UUID.randomUUID().toString());
        FileWriter fw = null;
        BufferedWriter bf = null;
        try {
            this.logger.info("Create temporary file {}.", outputFile);
            fw = new FileWriter(outputFile);
            bf = new BufferedWriter(fw);
            this.logger.info("Temporary file created.", outputFile);

            Gson jsonObject= new GsonBuilder().disableHtmlEscaping().create();
            this.logger.debug("Looping through the resultset, serializing and saving Json into file.");
            for (Map item : items) {
                try {
                    bf.write(jsonObject.toJson(item) + "\n");
                } catch (Exception e) {
                    this.logger.error(e.toString());
                    this.logger.error(item.toString());
                    throw new RuntimeException();
                }
            }
            bf.close();
        } catch (IOException e) {
            this.logger.error("Could not create the temporary file.");
            this.logger.error(e.toString());
            throw new RuntimeException();
        }
        this.logger.info("Exporting into JSON file completed successfully.");
        return outputFile;
    }

}
