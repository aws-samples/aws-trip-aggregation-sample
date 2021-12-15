import { Aws, NestedStack, RemovalPolicy, StackProps } from 'aws-cdk-lib';
import { CfnDeliveryStream } from 'aws-cdk-lib/aws-kinesisfirehose';
import { CfnDatabase, CfnTable } from 'aws-cdk-lib/aws-glue';
import { ArnPrincipal, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { join } from 'path';

export class DataIngestionStack extends NestedStack {

  /**
   * Bucket to store stream processed data
   */
  public readonly destinationBucket: Bucket;

  /**
   * Role that firehose will use for putting files in the destination bucket
   */
  public readonly ingestionRole: Role;

  public readonly glueDb: CfnDatabase;
  public readonly glueTable: CfnTable;

  /**
   * Stream where the data will be ingested
   */
  public deliveryStream: CfnDeliveryStream;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id);

    this.destinationBucket = new Bucket(this, 'IngestionDestinationBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
      // Uncomment these lines below to enable storage optimization
      // Configure a duration for data
      // lifecycleRules: [
      //   {
      //     enabled: true,
      //     expiration: Duration.days(30), // Maximum trip duration, defines the data to consume
      //   }
      // ]
    });

    this.glueDb = new CfnDatabase(this, 'GlueDatabase', {
      catalogId: Aws.ACCOUNT_ID,
      databaseInput: {}
    });

    this.glueTable = new CfnTable(this, 'GlueTable', {
      catalogId: Aws.ACCOUNT_ID,
      databaseName: this.glueDb.ref,
      tableInput: {
        owner: 'owner',
        retention: 0,
        storageDescriptor: {
          columns: [
            {
              name: 'EventId',
              type: 'string'
            },
            {
              name: 'TripId',
              type: 'string'
            },
            {
              name: 'DeviceId',
              type: 'string'
            },
            {
              name: 'EventDate',
              type: 'string'
            },
            {
              name: 'EventType',
              type: 'string'
            },
            {
              name: 'RandomData',
              type: 'string'
            }
          ],
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          compressed: false,
          numberOfBuckets: -1,
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            parameters: {
              'serialization.format': '1'
            }
          },
          bucketColumns: [],
          sortColumns: [],
          storedAsSubDirectories: false
        },
        partitionKeys: [
          {
            name: 'year',
            type: 'string'
          },
          {
            name: 'month',
            type: 'string'
          },
          {
            name: 'day',
            type: 'string'
          },
          {
            name: 'hour',
            type: 'string'
          },
          {
            name: 'minute',
            type: 'string'
          }
        ],
        tableType: 'EXTERNAL_TABLE'
      }
    });

    this.ingestionRole = new Role(this, 'IngestionRole', {
      assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
      inlinePolicies: {
        default: new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: [
                's3:*'
              ],
              resources: [
                this.destinationBucket.bucketArn,
                this.destinationBucket.arnForObjects('*')
              ]
            }),
            new PolicyStatement({
              actions: [
                'glue:GetTableVersions'
              ],
              resources: [
                '*'
              ]
            })
          ]
        })
      }
    });

    this.destinationBucket.addToResourcePolicy(new PolicyStatement({
      actions: [
        's3:*',
      ],
      resources: [
        this.destinationBucket.bucketArn,
        this.destinationBucket.arnForObjects('*')
      ],
      principals: [
        new ArnPrincipal(this.ingestionRole.roleArn)
      ]
    }));

    this.deliveryStream = new CfnDeliveryStream(this, 'TelemetryIngestionStream', {
      extendedS3DestinationConfiguration: {
        bucketArn: this.destinationBucket.bucketArn,
        bufferingHints: {
          sizeInMBs: 128, // Tune file size here
          intervalInSeconds: 60 // Tune maximum batch window here
        },
        compressionFormat: 'UNCOMPRESSED',
        errorOutputPrefix: `error/!{firehose:error-output-type}/dt=!{timestamp:yyyy'-'MM'-'dd}/h=!{timestamp:HH}/`,
        // prefix: '!{partitionKeyFromQuery:TripId}/', // Uncomment this line for by-trip partitioning
        prefix: `${join('year=!{partitionKeyFromQuery:year}',
                        'month=!{partitionKeyFromQuery:month}',
                        'day=!{partitionKeyFromQuery:day}',
                        'hour=!{partitionKeyFromQuery:hour}',
                        'minute=!{partitionKeyFromQuery:minute}')}/`, // Uncomment these lines for by-time partitioning
        roleArn: this.ingestionRole.roleArn,
        dynamicPartitioningConfiguration: {
          enabled: true,
          retryOptions: {
            durationInSeconds: 300
          },
        },
        processingConfiguration: {
          enabled: true,
          processors: [
            {
              type: 'MetadataExtraction',
              parameters: [
                {
                  parameterName: 'MetadataExtractionQuery',
                  parameterValue: '{ year : (.EventTime/1000) | strftime(\"%Y\"), month : (.EventTime/1000) | strftime(\"%m\"), day : (.EventTime/1000) | strftime(\"%d\"), hour: (.EventTime/1000) | strftime(\"%H\"), minute: (.EventTime/1000) | strftime(\"%M\"), TripId: .TripId }'
                },
                {
                  parameterName: 'JsonParsingEngine',
                  parameterValue: 'JQ-1.6'
                }
              ]
            },
            {
              type: 'AppendDelimiterToRecord',
              parameters: [
                {
                  parameterName: 'Delimiter',
                  parameterValue: '\\n'
                }
              ]
            }
          ]
        },
        dataFormatConversionConfiguration: {
          enabled: true,
          schemaConfiguration: {
            catalogId: Aws.ACCOUNT_ID,
            databaseName: this.glueDb.ref,
            tableName: this.glueTable.ref,
            roleArn: this.ingestionRole.roleArn
          },
          inputFormatConfiguration: {
            deserializer: {
              openXJsonSerDe: {}
            }
          },
          outputFormatConfiguration: {
            serializer: {
              parquetSerDe: {}
            }
          }
        }
      }
    });
  }
}
