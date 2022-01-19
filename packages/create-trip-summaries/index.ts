import { EventRecord } from './records';
import { S3, DynamoDB } from 'aws-sdk'
import Log from '@dazn/lambda-powertools-logger';
import { parse } from 'csv-parse/sync';

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
  Records: EventRecord[];
}

const TripSummariesTableName = process.env.TRIP_SUMMARIES_TABLE_NAME!;

const ddb = new DynamoDB.DocumentClient();

const s3 = new S3({
  signatureVersion: 'v4'
});

export const handler = async (event: any) => {
  
  Log.info(`Starting trip aggregation function`, {
    event
  });

  const queryExecutionResults = event.QueryResults;
  const queryResultFiles = queryExecutionResults
    .map((result: any) => result.QueryExecution.ResultConfiguration.OutputLocation)
    .map((result: any) => result.split('/').reverse().slice(0, 2))
    .map((result: any) => ({ Bucket: result[1], Key: result[0] }));


  const tripFile = queryResultFiles[1];

  Log.debug('Reading trip summary file', { File: queryResultFiles[0] });
  const tripSummaryFileResult = await s3.getObject({
    ...queryResultFiles[0],
  }).promise();

  Log.info('Processing trip summaries file');
  const tripSummaryFile = tripSummaryFileResult.Body!.toString();

  const tripsSummary: any[] = parse(tripSummaryFile, {
    columns: true,
    skipEmptyLines: true
  });

  // Add reference to trip file into records
  tripsSummary.forEach(trip => {
    trip.RecordsFile = tripFile
  });

  Log.info('Parsed trip summary', {
    summary: tripsSummary
  });

  const tripSummaryChunks: any[] = [];

  Log.info('Processing trip summaries', {
    TripCount: tripSummaryFile.length
  });

  for (let i = 0; i < tripsSummary.length; i += 25) {
    tripSummaryChunks.push(tripsSummary.slice(i, Math.min(i + 25, tripsSummary.length)));
  }

  Log.debug('Storing trip summaries', {
    TotalRequests: tripSummaryChunks.length
  });

  await Promise.all(tripSummaryChunks.map(async chunk => await ddb.batchWrite({
    RequestItems: {
      [TripSummariesTableName]: chunk.map((Item: any) => ({
        PutRequest: {
          Item
        }
      }))
    }
  }).promise()));

  Log.info('Trip summaries stored successfully');
};
