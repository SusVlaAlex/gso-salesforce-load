class DataInstanceFactory {
    public static DataInstance createDataInstance(String type){
        if (type.equalsIgnoreCase("dynamodb")){
            return new DynamoDBInstance();
        } else if (type.equalsIgnoreCase("salesforce")){
            return new SalesForceInstance();
        } else if (type.equalsIgnoreCase("s3")){
            return new S3Instance();
        }
        return null;
    }
}
