import { RemovalPolicy, Duration, Aws } from 'aws-cdk-lib';
import { ReadWriteType, Trail } from 'aws-cdk-lib/aws-cloudtrail';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Rule } from 'aws-cdk-lib/aws-events';
import { SfnStateMachine } from 'aws-cdk-lib/aws-events-targets';
import { PolicyStatement, PolicyStatementProps, ArnPrincipal, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Function, AssetCode, Runtime } from 'aws-cdk-lib/aws-lambda';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { IChainable, StateMachine, StateMachineType, JsonPath, LogLevel, IntegrationPattern, Parallel } from 'aws-cdk-lib/aws-stepfunctions';
import { AthenaStartQueryExecution, LambdaInvoke } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';
import { CfnWorkGroup } from 'aws-cdk-lib/aws-athena';

export interface DataReductionProps {
  RawDataBucketArn: string;
  AthenaDatabaseName: string;
  AthenaTableName: string;
}

export class DataReduction extends Construct {

  /**
   * Athena workgroup to store queries
   */
  public readonly tripReductionWorkGroup: CfnWorkGroup;

  /**
   * Stores trip information separated by trip
   */
  public readonly reducedTripBucket: Bucket;

  /**
   * Table to store trip summaries
   */
  public readonly tripSummaryTable: Table;

  /**
   * Trail to receive events for new raw data available
   */
  public readonly tripDataTrail: Trail;

  /**
   * Triggers the trip reduction functionality when new raw data is available
   */
  public readonly tripReductionStartRule: Rule;

  /**
   * This function prepares the payload needed for reducing trips
   */
  public readonly payloadPreparationFunction: Function;

  /**
   * Creates trip summaries once data is reduced
   */
  public readonly createTripSummariesFunction: Function;

  /**
   * Table to store trip information
   */
  public readonly tripsTable: Table;

  /**
   * Log group to store logs for trip reduction
   */
  public readonly tripReductionLogs: LogGroup;

  /**
   * Workflow to process new records
   */
  public readonly tripReductionStateMachine: StateMachine;

  constructor(scope: Construct, id: string, props: DataReductionProps) {
    super(scope, id);

    const rawDataBucket = Bucket.fromBucketArn(this, 'RawDataBucket', props.RawDataBucketArn);

    // Create bucket to store reduced trips
    this.reducedTripBucket = new Bucket(this, 'ReducedTripsBucket', {
      removalPolicy: RemovalPolicy.DESTROY
    });

    // Allow trip aggregation function to read bucket
    const readReducedTripBucketPolicy: PolicyStatementProps = {
      actions: [
        's3:GetObject'
      ],
      resources: [
        this.reducedTripBucket.arnForObjects('*')
      ]
    };

    this.tripReductionWorkGroup = new CfnWorkGroup(this, 'TripReductionWorkGroup', {
      name: 'TripReduction',
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${this.reducedTripBucket.bucketName}`
        },
      }
    });

    // Create trail to listen to events
    this.tripDataTrail = new Trail(this, 'TripDataTrail', {
      includeGlobalServiceEvents: false,
      isMultiRegionTrail: false,
      
    });

    this.tripDataTrail.addS3EventSelector([
      {
        bucket: rawDataBucket,
      }
    ], {
      includeManagementEvents: false,
      readWriteType: ReadWriteType.WRITE_ONLY
    });

    // Create trip summaries table
    this.tripSummaryTable = new Table(this, 'TripSummariesTable', {
      removalPolicy: RemovalPolicy.DESTROY,
      billingMode: BillingMode.PAY_PER_REQUEST,
      partitionKey: {
        name: 'trip_id',
        type: AttributeType.STRING
      }
    });

    // Create function for storing trip summaries
    this.createTripSummariesFunction = new Function(this, 'CreateTripSummariesFunction', {
      code: new AssetCode(`${__dirname}/../packages/create-trip-summaries`),
      handler: 'index.handler',
      runtime: Runtime.NODEJS_14_X,
      memorySize: 1024,
      timeout: Duration.seconds(15),
      environment: {
        TRIP_SUMMARIES_TABLE_NAME: this.tripSummaryTable.tableName
      }
    });

    // Grant function access to records bucket
    this.createTripSummariesFunction.addToRolePolicy(new PolicyStatement(readReducedTripBucketPolicy));
    this.reducedTripBucket.addToResourcePolicy(new PolicyStatement({
      ...readReducedTripBucketPolicy,
      principals: [
        new ArnPrincipal(this.createTripSummariesFunction.role!.roleArn)
      ]
    }));

    // Grant function write access on trip summaries table
    const putItemsPolicy: PolicyStatementProps = {
      actions: [
        'dynamodb:PutItem',
        'dynamodb:BatchWriteItem'
      ],
      resources: [
        this.tripSummaryTable.tableArn
      ]
    };

    this.createTripSummariesFunction.addToRolePolicy(new PolicyStatement(putItemsPolicy));

    // Create rule to start trip reductions
    this.tripReductionStartRule = new Rule(this, 'TripReductionStartRule', {
      description: 'Triggers when new raw data is available for trip reduction',
      enabled: true,
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['AWS API Call via CloudTrail'],
        detail: {
          eventSource: ['s3.amazonaws.com'],
          eventName: ['PutObject'],
          requestParameters: {
            bucketName: [rawDataBucket.bucketName]
          }
        }
      }
    });

    // Create payload preparation function
    this.payloadPreparationFunction = new Function(this, 'PayloadPreparationFunction', {
      code: new AssetCode(`${__dirname}/../packages/trip-reduction-function`),
      handler: 'index.handler',
      runtime: Runtime.NODEJS_14_X,
      environment: {
        ATHENA_TABLE_NAME: props.AthenaTableName
      }
    });

    /*
     * Create workflow tasks below
     */
    
    const preparePayloadTask = new LambdaInvoke(this, 'PreparePayloadTask', {
      lambdaFunction: this.payloadPreparationFunction,
      payloadResponseOnly: true
    });

    // Update Athena partitions
    const updateAthenaPartitionsTask = new AthenaStartQueryExecution(this, 'UpdateAthenaPartitionsTask', {
      queryString: `MSCK REPAIR TABLE ${props.AthenaTableName};`,
      comment: 'Updates Athena partitions using the new file received',
      resultPath: JsonPath.DISCARD,
      integrationPattern: IntegrationPattern.RUN_JOB,
      workGroup: this.tripReductionWorkGroup.ref,
      queryExecutionContext: {
        databaseName: props.AthenaDatabaseName
      }
    });

    // Find finished trips
    const findFinishedTripsTask = new AthenaStartQueryExecution(this, 'FindFinishedTripsTask', {
      queryString: JsonPath.stringAt(`States.Format($.FinishedTripsSummaryQuery, $.QueryFilterExpression)`),
      comment: 'Finds finished trips in the given timespan',
      integrationPattern: IntegrationPattern.RUN_JOB,
      workGroup: this.tripReductionWorkGroup.ref,
      queryExecutionContext: {
        databaseName: props.AthenaDatabaseName
      }
    });

    // Create bulk trips file
    const createTripsFileTask = new AthenaStartQueryExecution(this, 'CreateReducedTripsFileTask', {
      queryString: JsonPath.stringAt(`States.Format($.ReducedTripsQuery, $.QueryFilterExpression)`),
      comment: 'Unifies all records for finished trips in one file',
      integrationPattern: IntegrationPattern.RUN_JOB,
      workGroup: this.tripReductionWorkGroup.ref,
      queryExecutionContext: {
        databaseName: props.AthenaDatabaseName
      }
    });

    const parallelAthenaQueries = new Parallel(this, 'ParallelAthenaQueries', {
      comment: 'Runs Athena queries in parallel to improve performance',
      resultPath: '$.QueryResults'
    });

    const createTripSummariesTask = new LambdaInvoke(this, 'CreateTripSummariesTask', {
      comment: 'Creates a summary of all trips in Dynamo',
      lambdaFunction: this.createTripSummariesFunction
    });

    const parallelAggregationTasks = new Parallel(this, 'ParallelAggregationTasks', {
      comment: 'Runs aggregation procedures in parallel',
      resultPath: '$.AggregationResults'
    });

    /*
     * End create workflow tasks
     */

    // Create trip reduction flow
    const tripReductionFlow: IChainable = preparePayloadTask.next(
      updateAthenaPartitionsTask.next(
        parallelAthenaQueries.branch(
          findFinishedTripsTask,
          createTripsFileTask
        ).next(
          parallelAggregationTasks.branch(
            createTripSummariesTask,
            // Add other processes to data here
          )
        )
      )
    );

    // Create trip reduction log group
    this.tripReductionLogs = new LogGroup(this, 'TripReductionLogGroup', {
      removalPolicy: RemovalPolicy.DESTROY,
      retention: RetentionDays.ONE_WEEK
    });

    // Create trip reduction state machine
    this.tripReductionStateMachine = new StateMachine(this, 'TripReductionStateMachine', {
      definition: tripReductionFlow,
      stateMachineType: StateMachineType.STANDARD,
      logs: {
        destination: this.tripReductionLogs,
        level: LogLevel.ERROR
      }
    });

    this.tripReductionStateMachine.role.addToPrincipalPolicy(new PolicyStatement({
      actions: [
        'glue:BatchCreatePartition',
        'glue:BatchDeletePartition',
        'glue:BatchGetPartition',
        'glue:CreatePartition',
        'glue:DeletePartition',
        'glue:GetPartition',
        'glue:GetPartitions',
        'glue:GetTable',
        'glue:UpdatePartition',
      ],
      resources: [
        `arn:aws:glue:${Aws.REGION}:${Aws.ACCOUNT_ID}:database/${props.AthenaDatabaseName}`,
        `arn:aws:glue:${Aws.REGION}:${Aws.ACCOUNT_ID}:table/${props.AthenaDatabaseName}/${props.AthenaTableName}`
      ]
    }));

    this.tripReductionStateMachine.role.addToPrincipalPolicy(new PolicyStatement({
      actions: [
        's3:ListBucket'
      ],
      resources: [
        rawDataBucket.bucketArn,
        rawDataBucket.arnForObjects('*')
      ]
    }));

    rawDataBucket.addToResourcePolicy(new PolicyStatement({
      principals: [new ArnPrincipal(this.tripReductionStateMachine.role.roleArn)],
      actions: ['s3:ListBucket'],
      resources: [
        rawDataBucket.bucketArn,
        rawDataBucket.arnForObjects('*')
      ]
    }))

    const ruleRole = new Role(this, 'TripReductionStartRole', {
      assumedBy: new ServicePrincipal('events.amazonaws.com')
    });
    
    this.tripReductionStateMachine.grantStartExecution(ruleRole);

    // Add target to start trip reduction on new files
    this.tripReductionStartRule.addTarget(new SfnStateMachine(this.tripReductionStateMachine, {
      role: ruleRole
    }));
  }
}
