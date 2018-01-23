import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class DataObject {
    private final Logger logger = LoggerFactory.getLogger(DataObject.class);
    private String objectName;
    private DataInstance source;
    private final List<DataInstance> destinations = new ArrayList<>();

    DataObject(String objectName) {
        this.logger.info("Initializing the {} for {}.", DataObject.class ,objectName);
        this.objectName=objectName;
    }

    public void addDestination(DataInstance destination){
        logger.debug("Adding the destination into pipeline.");
        destinations.add(destination);
        logger.debug("New destination added: {}", destination.toString());
    }

    public void setSource(DataInstance instanceName){
        this.source=instanceName;
        //this.fields=source.describe(objectName);
    }

    public void copy(){
        ArrayList items = this.source.query(this.objectName);
        //Loops through all destionations and load item into each.
        for(DataInstance dest: destinations){
            dest.load(this.objectName, items);
        }
    }
}
