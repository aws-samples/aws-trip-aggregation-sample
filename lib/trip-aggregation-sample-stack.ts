import { CfnOutput, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { DataIngestionStack } from './data-ingestion';
import { DataReduction } from './data-reduction';
import { TripAggregationStack } from './trip-aggregation';

export class TripAggregationSampleStack extends Stack {

  /**
   * This construct handles the data ingestion features for this sample solution.
   * Devices ingest data through this construct.
   */
  public readonly dataIngestionConstruct: DataIngestionStack;

  /**
   * This construct helps reducing data from the multiple vehicles
   * into aggregated trip data.
   */
  public readonly dataReductionConstruct: DataReduction;

  /**
   * This construct manages the trip aggregation logic, ran once trips have finished.
   * The device simulator notifies once trips are finished and ready for processing,
   * and the logic runs reactively to process trips.
   */
  public readonly tripAggregationConstruct: TripAggregationStack;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Initialize data ingestion
    this.dataIngestionConstruct = new DataIngestionStack(this, 'DataIngestionConstruct');

    // Initialize data reduction
    this.dataReductionConstruct = new DataReduction(this, 'DataReduction', {
      AthenaDatabaseName: 'trip_aggregation_sample',
      AthenaTableName: 'aggregated_trips',
      RawDataBucketArn: this.dataIngestionConstruct.destinationBucket.bucketArn,
    });

    // Initialize trip aggregation
    this.tripAggregationConstruct = new TripAggregationStack(this, 'TripAggregationConstruct', {
      ReducedTripDataBucketName: this.dataReductionConstruct.reducedTripBucket.bucketName,
      TripSummariesTableArn: this.dataReductionConstruct.tripSummaryTable.tableArn
    });

    // Output variables to use later
    new CfnOutput(this, 'DeliveryStreamName', { value: this.dataIngestionConstruct.deliveryStream.ref });
  }
}
