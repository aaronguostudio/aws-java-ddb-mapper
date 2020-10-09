import com.amazonaws.services.dynamodbv2.datamodeling.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import model.Customer;
import translators.CustomerTranslator;

@ToString
@NoArgsConstructor
@Getter
@Setter
@DynamoDBTable(tableName = "transactions")
public class Transaction {

  @DynamoDBHashKey(attributeName = "transactionId")
  private String transactionId;

  @DynamoDBRangeKey(attributeName = "date")
  @DynamoDBIndexHashKey(attributeName = "date", globalSecondaryIndexName = "date-index")
  private String date;

  @DynamoDBAttribute(attributeName = "amount")
  private Integer amount;

  @DynamoDBAttribute(attributeName = "type")
  private String type;

  @DynamoDBVersionAttribute(attributeName = "version")
  private Long version;

  @DynamoDBAttribute(attributeName = "customer")
  @DynamoDBTypeConverted(converter = CustomerTranslator.class)
  private Customer customer;
}

