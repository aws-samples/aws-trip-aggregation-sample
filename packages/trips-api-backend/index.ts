import { EventRecord } from './records';
import { S3, DynamoDB } from 'aws-sdk'
import Log from '@dazn/lambda-powertools-logger';

export interface ApiEvent {
  TripId: string;
}

export interface Trip {
  
  /**
   * Unique Id of the trip
   */
  Id: string;

  /**
   * Id of the vehicle driving this trip
   */
  DeviceId: string;

  /**
   * Start date for the trip
   */
  StartDate: string;

  /**
   * End date for the trip
   */
  EndDate: string;

  /**
   * Amount of events received, vs amount of events that should be available - i.e. 1 per second.
   */
  DataIntegrityRate: number;

  /**
   * Telemetry records for this trip
   */
  Records?: EventRecord[];

  /**
   * File where the reduced records for this trip is located
   */
  RecordsFile?: { Bucket: string, Key: string };

  /**
   * Whether the trip aggregation process has been done for this trip
   */
  AggregationExecuted?: boolean;
}


const TripSummariesTableName = process.env.TRIP_SUMMARIES_TABLE_NAME!;
const ReducedTripDataBucketName = process.env.REDUCED_TRIP_DATA_BUCKET_NAME!;
const TripRecordsBucketName = process.env.TRIP_RECORDS_BUCKET_NAME!;

const s3 = new S3({
  signatureVersion: 'v4'
});

const dynamodb = new DynamoDB.DocumentClient();

export const handler = async (event: ApiEvent) => {
  Log.info('Starting trips API backend function', { event });

  // Get trip summary information
  Log.debug('Fetching trip summary information', { TripId: event.TripId })
  const tripResponse = await dynamodb.get({
    TableName: TripSummariesTableName,
    Key: {
      trip_id: event.TripId
    }
  }).promise();

  const tripSummary: Trip = tripResponse.Item! as any;
  let aggregatedTrip: Trip = tripSummary;

  // Verify if trip aggregation has been done already
  Log.debug('Summary obtained. Verifying trip aggregation status', { TripSummary: tripSummary });
  if (!tripSummary.AggregationExecuted) {
    let tripRecords: EventRecord[] = [];
    Log.debug('Aggregation has not been done yet. Executing now');

    // Fetch reduced trip data
    const reducedDataResponse = await s3.selectObjectContent({
      Bucket: tripSummary.RecordsFile!.Bucket,
      Key: tripSummary.RecordsFile!.Key,
      ExpressionType: 'SQL',
      Expression: `select  * from s3object s where s."tripid" = '${event.TripId}'`,
      InputSerialization: {
        CSV: {
          FileHeaderInfo: 'Use',
        }
      },
      OutputSerialization: {
        JSON: {

        }
      }
    }).promise();

    const tripRecordsStream = reducedDataResponse.Payload! as any;
    
    for await (var record of tripRecordsStream) {
      if (record.Records) {
        // handle Records record
        const parsedRecordsString = record.Records.Payload.toString('utf-8');
        const parsedRecords = parsedRecordsString
          .split('\n')
          .filter((r: string) => r && r.length)
          .map((r: string) => {
            try {
              return JSON.parse(r);
            } catch (e) {
              return null
            }
          })
          .filter((r: any) => !!r);
        
        tripRecords = tripRecords.concat(parsedRecords);
      } else if (record.End) {
        // handle End record
        Log.debug('Finished', { records: record.End });
      }
    }

    Log.debug('Trip records fetched successfully', { records: tripRecords.length });
    const enhancedTrip: Trip = {
      ...tripSummary,
      Records: tripRecords,
    };

    // Store aggregated trip
    Log.debug('Storing aggregated trip in S3');
    await s3.putObject({
      Bucket: TripRecordsBucketName,
      Key: `trips/${event.TripId}.json`,
      Body: JSON.stringify(enhancedTrip)
    }).promise();

    // Update trip summary
    await dynamodb.update({
      TableName: TripSummariesTableName,
      Key: {
        TripId: event.TripId
      },
      UpdateExpression: 'set AggregationExecuted = :value',
      ExpressionAttributeValues: {
        ':value': true
      }
    }).promise();

    aggregatedTrip = enhancedTrip;
  } else {
    Log.info('Aggregation was already executed before. Fetching file');
    const aggregatedTripResponse = await s3.getObject({
      Bucket: TripRecordsBucketName,
      Key: `trips/${event.TripId}.json`
    }).promise();

    aggregatedTrip = JSON.parse(aggregatedTripResponse.Body!.toString('utf-8'));
  }

  return aggregatedTrip;
};
