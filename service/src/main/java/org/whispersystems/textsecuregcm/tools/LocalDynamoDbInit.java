/*
 * Local DynamoDB table initializer for development environments.
 */
package org.whispersystems.textsecuregcm.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public final class LocalDynamoDbInit {

  private static final ProvisionedThroughput DEFAULT_PROVISIONED_THROUGHPUT = ProvisionedThroughput.builder()
      .readCapacityUnits(20L)
      .writeCapacityUnits(20L)
      .build();

  public static void main(final String[] args) throws Exception {
    String configPath = null;
    String endpoint = null;
    for (int i = 0; i < args.length; i++) {
      if ("--config".equals(args[i]) && i + 1 < args.length) {
        configPath = args[++i];
      } else if ("--endpoint".equals(args[i]) && i + 1 < args.length) {
        endpoint = args[++i];
      }
    }

    if (configPath == null || configPath.isBlank()) {
      throw new IllegalArgumentException("Missing --config <path>");
    }
    if (endpoint == null || endpoint.isBlank()) {
      endpoint = "http://127.0.0.1:8000";
    }

    final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    final JsonNode root = mapper.readTree(new File(configPath));
    final JsonNode tables = root.path("dynamoDbTables");

    if (tables.isMissingNode()) {
      throw new IllegalArgumentException("dynamoDbTables not found in config: " + configPath);
    }

    final DynamoDbClient client = DynamoDbClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .region(Region.of("local"))
        .endpointOverride(URI.create(endpoint))
        .build();

    final List<CreateRequest> requests = new ArrayList<>();

    requests.add(new CreateRequest(tables.path("accounts").path("tableName").asText(null), Tables.ACCOUNTS));
    requests.add(new CreateRequest(tables.path("accounts").path("phoneNumberTableName").asText(null), Tables.NUMBERS));
    requests.add(new CreateRequest(tables.path("accounts").path("phoneNumberIdentifierTableName").asText(null), Tables.PNI_ASSIGNMENTS));
    requests.add(new CreateRequest(tables.path("accounts").path("usernamesTableName").asText(null), Tables.USERNAMES));
    requests.add(new CreateRequest(tables.path("accounts").path("usedLinkDeviceTokensTableName").asText(null), Tables.USED_LINK_DEVICE_TOKENS));

    requests.add(new CreateRequest(tables.path("appleDeviceChecks").path("tableName").asText(null), Tables.APPLE_DEVICE_CHECKS));
    requests.add(new CreateRequest(tables.path("appleDeviceCheckPublicKeys").path("tableName").asText(null), Tables.APPLE_DEVICE_CHECKS_KEY_CONSTRAINT));
    requests.add(new CreateRequest(tables.path("backups").path("tableName").asText(null), Tables.BACKUPS));
    requests.add(new CreateRequest(tables.path("clientReleases").path("tableName").asText(null), Tables.CLIENT_RELEASES));
    requests.add(new CreateRequest(tables.path("deletedAccounts").path("tableName").asText(null), Tables.DELETED_ACCOUNTS));
    requests.add(new CreateRequest(tables.path("deletedAccountsLock").path("tableName").asText(null), Tables.DELETED_ACCOUNTS_LOCK));
    requests.add(new CreateRequest(tables.path("issuedReceipts").path("tableName").asText(null), Tables.ISSUED_RECEIPTS));
    requests.add(new CreateRequest(tables.path("ecKeys").path("tableName").asText(null), Tables.EC_KEYS));
    requests.add(new CreateRequest(tables.path("ecSignedPreKeys").path("tableName").asText(null), Tables.REPEATED_USE_EC_SIGNED_PRE_KEYS));
    requests.add(new CreateRequest(tables.path("pagedPqKeys").path("tableName").asText(null), Tables.PAGED_PQ_KEYS));
    requests.add(new CreateRequest(tables.path("pqLastResortKeys").path("tableName").asText(null), Tables.REPEATED_USE_KEM_SIGNED_PRE_KEYS));
    requests.add(new CreateRequest(tables.path("messages").path("tableName").asText(null), Tables.MESSAGES));
    requests.add(new CreateRequest(tables.path("onetimeDonations").path("tableName").asText(null), Tables.ONETIME_DONATIONS));
    requests.add(new CreateRequest(tables.path("phoneNumberIdentifiers").path("tableName").asText(null), Tables.PNI));
    requests.add(new CreateRequest(tables.path("profiles").path("tableName").asText(null), Tables.PROFILES));
    requests.add(new CreateRequest(tables.path("pushChallenge").path("tableName").asText(null), Tables.PUSH_CHALLENGES));
    requests.add(new CreateRequest(tables.path("pushNotificationExperimentSamples").path("tableName").asText(null), Tables.PUSH_NOTIFICATION_EXPERIMENT_SAMPLES));
    requests.add(new CreateRequest(tables.path("redeemedReceipts").path("tableName").asText(null), Tables.REDEEMED_RECEIPTS));
    requests.add(new CreateRequest(tables.path("registrationRecovery").path("tableName").asText(null), Tables.REGISTRATION_RECOVERY_PASSWORDS));
    requests.add(new CreateRequest(tables.path("remoteConfig").path("tableName").asText(null), Tables.REMOTE_CONFIGS));
    requests.add(new CreateRequest(tables.path("reportMessage").path("tableName").asText(null), Tables.REPORT_MESSAGES));
    requests.add(new CreateRequest(tables.path("scheduledJobs").path("tableName").asText(null), Tables.SCHEDULED_JOBS));
    requests.add(new CreateRequest(tables.path("subscriptions").path("tableName").asText(null), Tables.SUBSCRIPTIONS));
    requests.add(new CreateRequest(tables.path("clientPublicKeys").path("tableName").asText(null), Tables.CLIENT_PUBLIC_KEYS));
    requests.add(new CreateRequest(tables.path("verificationSessions").path("tableName").asText(null), Tables.VERIFICATION_SESSIONS));

    for (final CreateRequest request : requests) {
      if (request.tableName == null || request.tableName.isBlank()) {
        continue;
      }
      createTable(client, request);
    }
  }

  private static void createTable(final DynamoDbClient client, final CreateRequest request) {
    final Tables schema = request.schema;
    final List<KeySchemaElement> keySchemaElements = new ArrayList<>();
    keySchemaElements.add(KeySchemaElement.builder()
        .attributeName(schema.hashKeyName())
        .keyType(KeyType.HASH)
        .build());
    if (schema.rangeKeyName() != null) {
      keySchemaElements.add(KeySchemaElement.builder()
          .attributeName(schema.rangeKeyName())
          .keyType(KeyType.RANGE)
          .build());
    }

    final CreateTableRequest createTableRequest = CreateTableRequest.builder()
        .tableName(request.tableName)
        .keySchema(keySchemaElements)
        .attributeDefinitions(schema.attributeDefinitions().isEmpty() ? null : schema.attributeDefinitions())
        .globalSecondaryIndexes(schema.globalSecondaryIndexes().isEmpty() ? null : schema.globalSecondaryIndexes())
        .localSecondaryIndexes(schema.localSecondaryIndexes().isEmpty() ? null : schema.localSecondaryIndexes())
        .provisionedThroughput(DEFAULT_PROVISIONED_THROUGHPUT)
        .build();

    try {
      client.createTable(createTableRequest);
    } catch (ResourceInUseException ignored) {
      // table already exists
    }
  }

  private record CreateRequest(String tableName, Tables schema) {}

  private interface TableSchema {
    String hashKeyName();
    String rangeKeyName();
    List<AttributeDefinition> attributeDefinitions();
    List<GlobalSecondaryIndex> globalSecondaryIndexes();
    List<LocalSecondaryIndex> localSecondaryIndexes();
  }

  private enum Tables implements TableSchema {
    ACCOUNTS(
        "U",
        null,
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("UL")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(
            GlobalSecondaryIndex.builder()
                .indexName("ul_to_u")
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName("UL")
                        .keyType(KeyType.HASH)
                        .build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
                .build()
        ),
        List.of()),

    BACKUPS(
        "U",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("U")
            .attributeType(ScalarAttributeType.B).build()),
        List.of(), List.of()),

    CLIENT_RELEASES(
        "P",
        "V",
        List.of(
            AttributeDefinition.builder()
                .attributeName("P")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("V")
                .attributeType(ScalarAttributeType.S)
                .build()),
        List.of(),
        List.of()),

    DELETED_ACCOUNTS(
        "P",
        null,
        List.of(
            AttributeDefinition.builder()
                .attributeName("P")
                .attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(
            GlobalSecondaryIndex.builder()
                .indexName("u_to_p")
                .keySchema(
                    KeySchemaElement.builder().attributeName("U").keyType(KeyType.HASH).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
                .build()),
        List.of()
    ),

    DELETED_ACCOUNTS_LOCK(
        "P",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("P")
            .attributeType(ScalarAttributeType.S).build()),
        List.of(), List.of()),

    NUMBERS(
        "P",
        null,
        List.of(AttributeDefinition.builder()
              .attributeName("P")
              .attributeType(ScalarAttributeType.S)
            .build()),
        List.of(), List.of()),

    EC_KEYS(
        "U",
        "DK",
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("DK")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(), List.of()),

    PAGED_PQ_KEYS(
        "U",
        "D",
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("D")
                .attributeType(ScalarAttributeType.N)
                .build()),
        List.of(), List.of()),

    PUSH_NOTIFICATION_EXPERIMENT_SAMPLES(
        "N",
        "AD",
        List.of(
            AttributeDefinition.builder()
                .attributeName("N")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("AD")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(), List.of()),

    REPEATED_USE_EC_SIGNED_PRE_KEYS(
        "U",
        "D",
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("D")
                .attributeType(ScalarAttributeType.N)
                .build()),
        List.of(), List.of()),

    REPEATED_USE_KEM_SIGNED_PRE_KEYS(
        "U",
        "D",
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("D")
                .attributeType(ScalarAttributeType.N)
                .build()),
        List.of(), List.of()),

    PNI(
        "P",
        null,
        List.of(
            AttributeDefinition.builder()
                .attributeName("P")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("PNI")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(GlobalSecondaryIndex.builder()
            .indexName("pni_to_p")
            .projection(Projection.builder()
                .projectionType(ProjectionType.KEYS_ONLY)
                .build())
            .keySchema(KeySchemaElement.builder().keyType(KeyType.HASH)
                .attributeName("PNI")
                .build())
            .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
            .build()),
        List.of()),

    PNI_ASSIGNMENTS(
        "PNI",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("PNI")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(), List.of()),

    ISSUED_RECEIPTS(
        "A",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("A")
            .attributeType(ScalarAttributeType.S)
            .build()),
        List.of(), List.of()),

    MESSAGES(
        "H",
        "S",
        List.of(
            AttributeDefinition.builder().attributeName("H").attributeType(ScalarAttributeType.B).build(),
            AttributeDefinition.builder().attributeName("S").attributeType(ScalarAttributeType.B).build()),
        List.of(), List.of()),

    ONETIME_DONATIONS(
        "P",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("P")
            .attributeType(ScalarAttributeType.S)
            .build()),
        List.of(), List.of()),

    PROFILES(
        "U",
        "V",
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("V")
                .attributeType(ScalarAttributeType.S)
                .build()),
        List.of(), List.of()),

    PUSH_CHALLENGES(
        "U",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("U")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(), List.of()),

    REDEEMED_RECEIPTS(
        "S",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("S")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(), List.of()),

    REGISTRATION_RECOVERY_PASSWORDS(
        "P",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("P")
            .attributeType(ScalarAttributeType.S)
            .build()),
        List.of(), List.of()),

    REMOTE_CONFIGS(
        "N",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("N")
            .attributeType(ScalarAttributeType.S)
            .build()),
        List.of(), List.of()),

    REPORT_MESSAGES(
        "H",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("H")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(), List.of()),

    SCHEDULED_JOBS(
        "S",
        "T",
        List.of(AttributeDefinition.builder()
                .attributeName("S")
                .attributeType(ScalarAttributeType.S)
                .build(),

            AttributeDefinition.builder()
                .attributeName("T")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(),
        List.of()),

    SUBSCRIPTIONS(
        "U",
        null,
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("PC")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(GlobalSecondaryIndex.builder()
            .indexName("pc_to_u")
            .keySchema(KeySchemaElement.builder()
                .attributeName("PC")
                .keyType(KeyType.HASH)
                .build())
            .projection(Projection.builder()
                .projectionType(ProjectionType.KEYS_ONLY)
                .build())
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(20L)
                .writeCapacityUnits(20L)
                .build())
            .build()),
        List.of()),

    CLIENT_PUBLIC_KEYS(
        "U",
        "D",
        List.of(
            AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("D")
                .attributeType(ScalarAttributeType.N)
                .build()),
        List.of(),
        List.of()),

    USED_LINK_DEVICE_TOKENS(
        "H",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("H")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(),
        List.of()),

    USERNAMES(
        "N",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("N")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(), List.of()),

    VERIFICATION_SESSIONS(
        "K",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("K")
            .attributeType(ScalarAttributeType.S)
            .build()),
        List.of(), List.of()),

    APPLE_DEVICE_CHECKS(
        "U",
        "KID",
        List.of(AttributeDefinition.builder()
                .attributeName("U")
                .attributeType(ScalarAttributeType.B)
                .build(),
            AttributeDefinition.builder()
                .attributeName("KID")
                .attributeType(ScalarAttributeType.B)
                .build()),
        List.of(), List.of()),

    APPLE_DEVICE_CHECKS_KEY_CONSTRAINT(
        "PK",
        null,
        List.of(AttributeDefinition.builder()
            .attributeName("PK")
            .attributeType(ScalarAttributeType.B)
            .build()),
        List.of(), List.of());

    private final String hashKeyName;
    private final String rangeKeyName;
    private final List<AttributeDefinition> attributeDefinitions;
    private final List<GlobalSecondaryIndex> globalSecondaryIndexes;
    private final List<LocalSecondaryIndex> localSecondaryIndexes;

    Tables(
        final String hashKeyName,
        final String rangeKeyName,
        final List<AttributeDefinition> attributeDefinitions,
        final List<GlobalSecondaryIndex> globalSecondaryIndexes,
        final List<LocalSecondaryIndex> localSecondaryIndexes
    ) {
      this.hashKeyName = hashKeyName;
      this.rangeKeyName = rangeKeyName;
      this.attributeDefinitions = attributeDefinitions;
      this.globalSecondaryIndexes = globalSecondaryIndexes;
      this.localSecondaryIndexes = localSecondaryIndexes;
    }

    public String hashKeyName() {
      return hashKeyName;
    }

    public String rangeKeyName() {
      return rangeKeyName;
    }

    public List<AttributeDefinition> attributeDefinitions() {
      return attributeDefinitions;
    }

    public List<GlobalSecondaryIndex> globalSecondaryIndexes() {
      return globalSecondaryIndexes;
    }

    public List<LocalSecondaryIndex> localSecondaryIndexes() {
      return localSecondaryIndexes;
    }
  }
}
