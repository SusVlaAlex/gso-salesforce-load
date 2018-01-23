import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Pavlo_Suslykov on 6/2/2017.
 */
public class Main {
    public static void main(String[] args){
        Logger logger = LoggerFactory.getLogger(Main.class);
        String syncObjectName=null;
        String source=null;
        String[] targets=null;

        //Getting input arguments
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(setCmdOptions(),args);
            //Initialize input values
            syncObjectName = cmd.getOptionValue("object");
            source = cmd.getOptionValue("source");
            targets = cmd.getOptionValues("target");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //Running the sync job.
        logger.info("Loading SalesForce object '{}'.", syncObjectName);
        DataObject obj = new DataObject(syncObjectName);
        for(String tgt: targets){
            logger.info("Adding the target {}", tgt);
            obj.addDestination(DataInstanceFactory.createDataInstance(tgt));
        }
        obj.setSource(DataInstanceFactory.createDataInstance(source));
        obj.copy();
    }

    static private Options setCmdOptions(){
        Options cmdOptions = new Options();
        cmdOptions.addOption("object", true, "Object name that should be synced");
        cmdOptions.addOption("source", true, "Source system definition.");
        cmdOptions.addOption("target", true, "The list for target systems.");
        cmdOptions.getOption("target").setArgs(Option.UNLIMITED_VALUES);
        return cmdOptions;
    }

}
