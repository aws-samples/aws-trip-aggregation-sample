import { Aws, Duration, NestedStack, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { LambdaIntegration, PassthroughBehavior, RestApi } from 'aws-cdk-lib/aws-apigateway';
import { AttributeType, BillingMode, ITable, Table } from 'aws-cdk-lib/aws-dynamodb';
import { PolicyStatement, PolicyStatementProps, ArnPrincipal, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { CfnTopicRule } from 'aws-cdk-lib/aws-iot';
import { AssetCode, CfnPermission, Function, Permission, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Bucket, IBucket } from 'aws-cdk-lib/aws-s3';
import { HttpMethod } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

export interface TripAggregationProps extends StackProps {

  TripSummariesTableArn: string;
  ReducedTripDataBucketName: string;
}

export class TripAggregationStack extends NestedStack {

  /**
   * This bucket stores the aggregated information for all trips
   */
  public readonly tripRecordsBucket: Bucket;

  /**
   * API to fetch aggregated trips
   */
  public readonly aggregatedTripsApi: RestApi;

  /**
   * API Backend function
   */
  public readonly apiBackendFunction: Function;

  constructor(scope: Construct, id: string, props: TripAggregationProps) {
    super(scope, id);

    const reducedTripDataBucket: IBucket = Bucket.fromBucketName(this, 'ReducedTripDataBucket', props.ReducedTripDataBucketName);
    const tripSummariesTable: ITable = Table.fromTableArn(this, 'TripSummariesTable', props.TripSummariesTableArn);

    // Create bucket for storing aggregated trips
    this.tripRecordsBucket = new Bucket(this, 'AggregatedTripsBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create function for storing trip records
    this.apiBackendFunction = new Function(this, 'ApiBackendFunction', {
      code: new AssetCode(`${__dirname}/../packages/trips-api-backend`),
      handler: 'index.handler',
      runtime: Runtime.NODEJS_14_X,
      memorySize: 1024,
      timeout: Duration.seconds(30),
      environment: {
        TRIP_SUMMARIES_TABLE_NAME: tripSummariesTable.tableName,
        TRIP_RECORDS_BUCKET_NAME: this.tripRecordsBucket.bucketName,
        REDUCED_TRIP_DATA_BUCKET_NAME: props.ReducedTripDataBucketName
      }
    });

    // Grant function read/write access to trip summaries table
    const tripSummariesPolicy: PolicyStatementProps = {
      actions: [
        'dynamodb:GetItem',
        'dynamodb:UpdateItem'
      ],
      resources: [
        tripSummariesTable.tableArn
      ]
    };
    this.apiBackendFunction.addToRolePolicy(new PolicyStatement(tripSummariesPolicy));

    // Grant function read access to reduced data bucket
    const reducedTripsPolicy: PolicyStatementProps = {
      actions: [
        's3:GetObject',
      ],
      resources: [
        reducedTripDataBucket.arnForObjects('*')
      ]
    };

    this.apiBackendFunction.addToRolePolicy(new PolicyStatement(reducedTripsPolicy));
    reducedTripDataBucket.addToResourcePolicy(new PolicyStatement({
      ...reducedTripsPolicy,
      principals: [
        new ArnPrincipal(this.apiBackendFunction.role!.roleArn)
      ]
    }));

    // Grant function read/write access to aggregated data
    const aggregatedTripsPolicy: PolicyStatementProps = {
      actions: [
        's3:GetObject',
        's3:PutObject'
      ],
      resources: [
        this.tripRecordsBucket.arnForObjects('*')
      ]
    };

    this.apiBackendFunction.addToRolePolicy(new PolicyStatement(aggregatedTripsPolicy));
    this.tripRecordsBucket.addToResourcePolicy(new PolicyStatement({
      ...aggregatedTripsPolicy,
      principals: [
        new ArnPrincipal(this.apiBackendFunction.role!.roleArn)
      ]
    }));

    // Initialize aggregated trips API
    this.aggregatedTripsApi = new RestApi(this, 'AggregatedTripsApi');
    
    const tripsResource = this.aggregatedTripsApi.root.addResource('trips');
    const byTripIdResource = tripsResource.addResource('{tripId}');

    byTripIdResource.addMethod(HttpMethod.GET, new LambdaIntegration(this.apiBackendFunction, {
      proxy: false,
      passthroughBehavior: PassthroughBehavior.WHEN_NO_TEMPLATES,
      requestTemplates: {
        'application/json': JSON.stringify({
          TripId: `$input.params('tripId')`
        })
      },
      integrationResponses: [
        {
          statusCode: '200',
        }
      ]
    }), {
      requestParameters: {
        'method.request.path.tripId': true
      },
      methodResponses: [
        {
          statusCode: '200'
        }
      ]
    });
  }
}
