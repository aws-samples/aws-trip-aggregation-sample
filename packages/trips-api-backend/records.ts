
/**
* Describes a record of a device telemetry event
*/
export interface EventRecord {
  
  /**
  * Id of the event record
  */
  EventId: string;
  
  /**
  * Id of the trip this record refers to
  */
  TripId: string;
  
  /**
  * Id of the vehicle doing this trip
  */
  DeviceId: string;
  
  /**
  * Time when the event is dispatched
  */
  EventTime: number;
  EventDate: string;
  
  /**
  * Type of event
  */
  EventType: EventType;
  
  /**
  * This object can contain random information to size up the payload of the event
  */
  RandomData?: string;
}

export enum EventType {
  ENGINE_START = 'engine-start',
  KEEP_ALIVE = 'keep-alive',
  TRIP_FINISHED = 'trip-finished'
}