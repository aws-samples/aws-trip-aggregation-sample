import { StepFunctions } from 'aws-sdk'
import Log from '@dazn/lambda-powertools-logger';
import { EventBridgeEvent } from 'aws-lambda';

export interface InitialInput {
  bucketName: string;
  key: string;
}

export interface StateMachineInput {
  FinishedTripsSummaryQuery: string;
  ReducedTripsQuery: string;
  QueryFilterExpression: string;
  FormattedDate: string;
}

const AthenaTableName = process.env.ATHENA_TABLE_NAME!;

export const handler = async (event: EventBridgeEvent<any, any>) => {
  Log.info('Processing new incoming partition', {
    Event: event
  });

  const realEvent: InitialInput = event.detail.requestParameters

  const rawStateMachineInput: any = {
    year: '',
    month: '',
    day: '',
    hour: '',
    minute: ''
  }

  // Find date to process
  const objectKey = realEvent.key;
  const keyParams = objectKey.split('/');
  keyParams.forEach(param => {
    const paramParams = param.split('=');
    if(paramParams.length < 2) return;

    const paramName = paramParams[0];
    const paramValue = paramParams[1];
    rawStateMachineInput[paramName] = paramValue;
  });

  const stateMachineInput: StateMachineInput = {
    FinishedTripsSummaryQuery: `SELECT 
      a.deviceid as vehicle_id,
      a.tripid as trip_id,
      b.eventdate as start_date,
      a.eventdate as end_date,
      date_diff('second', from_iso8601_timestamp(b.eventdate), from_iso8601_timestamp(a.eventdate)) as duration,
      (select count(1) from "${AthenaTableName}" c where a.tripid = c.tripid) as event_count
    FROM "${AthenaTableName}" a
    LEFT JOIN "${AthenaTableName}" b
      on a.tripid = b.tripid and b.eventtype = 'engine-start'
    where 
      a.eventtype = 'trip-finished'
      and {};`,
    ReducedTripsQuery: `SELECT *
      FROM "${AthenaTableName}" a
      where tripid in (SELECT tripid 
          FROM "${AthenaTableName}" a
          where a.eventtype = 'trip-finished' 
              and {})
      order by tripid, eventdate;`,
    QueryFilterExpression: [
      `a.year = '${rawStateMachineInput.year}'`,
      `a.month = '${rawStateMachineInput.month}'`,
      `a.day = '${rawStateMachineInput.day}'`,
      `a.hour = '${rawStateMachineInput.hour}'`,
      `a.minute = '${rawStateMachineInput.minute}'`
    ].join(' and '),
    FormattedDate: [
      rawStateMachineInput.year,
      rawStateMachineInput.month,
      rawStateMachineInput.day,
      rawStateMachineInput.hour,
      rawStateMachineInput.minute
    ].join('-')
  }

  Log.info('Successfully prepared execution input', {
    input: stateMachineInput
  });

  return stateMachineInput;
};
