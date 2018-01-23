import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Pavlo_Suslykov on 6/2/2017.
 */
public class Main {
    public static void main(String[] args){
        Logger logger = LoggerFactory.getLogger(Main.class);

        //Parsing the input arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(setCmdOptions(),args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String sfobject = cmd.getOptionValue("object");
        String source = cmd.getOptionValue("source");
        String[] targets = cmd.getOptionValues("target");

        //Initializing and running the sync job.
        logger.info("Loading SalesForce object '{}'.", sfobject);
        DataObject obj = new DataObject(sfobject);
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
