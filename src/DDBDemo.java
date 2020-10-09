import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import env.EnvironmentVariables;
import model.Customer;

import java.util.List;
import java.util.zip.DataFormatException;

public class DDBDemo {
  public static void main (String[] args) {
//    AWSCredentialsProvider creds = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
//            EnvironmentVariables.ACCESS_KEY,
//            EnvironmentVariables.SECRET_KEY));

    AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard()
            .withRegion("us-east-1")
            .build();

    DynamoDBMapper mapper = new DynamoDBMapper(ddbClient);
    load(mapper);
    save(mapper);
    query(mapper);
    delete(mapper);
  }

  private static void load(DynamoDBMapper mapper) {
    Transaction t = new Transaction();
    t.setTransactionId("t1");
    t.setDate("2020-03-01");

    Transaction result = mapper.load(t);
    System.out.println(result);
  }
  private static void save(DynamoDBMapper mapper) {
    // 1 - simple save
    Transaction t = new Transaction();
    t.setTransactionId("t3");
    t.setDate("2020-10-20");
//    t.setAmount(999);
//    t.setType("PURCHASE");
//    t.setCustomer(Customer.builder().customerId("c2").customerName("CiCi").build());
//    mapper.save(t);

    // 2 - get and save
    Transaction result = mapper.load(t);
    result.setAmount(555);
    mapper.save(result);

    // 3 - batch saving
    mapper.batchSave(t,t,t);
  }
  private static void query(DynamoDBMapper mapper) {
    // 1 - normal query
    Transaction t = new Transaction();
    t.setTransactionId("t1");

    DynamoDBQueryExpression<Transaction> queryExpression =
            new DynamoDBQueryExpression<Transaction>()
              .withHashKeyValues(t)
              .withLimit(10);

    List<Transaction> result = mapper.query(Transaction.class, queryExpression);

    System.out.println(">> Query:");
    System.out.println(result.size());
    System.out.println(result.get(0));
    System.out.println(result.get(1));

    // 2 - GSI
    Transaction t2 = new Transaction();
    t.setDate("2020-03-01");

    DynamoDBQueryExpression<Transaction> queryExpression2 =
            new DynamoDBQueryExpression<Transaction>()
              .withHashKeyValues(t)
              .withIndexName("date-index")
              .withConsistentRead(false)
              .withLimit(10);

    List<Transaction> result2 = mapper.query(Transaction.class, queryExpression2);
    System.out.println(">> Query GSI");
    System.out.println(result2.size());
    System.out.println(result2.get(0));
  }
  private static void delete(DynamoDBMapper mapper) {
    Transaction t = new Transaction();
    t.setTransactionId("t3");
    t.setDate("2020-10-20");

    Transaction result = mapper.load(t);
    mapper.delete(result);

    // mapper.batchDelete(t,t,t);
  }
}
