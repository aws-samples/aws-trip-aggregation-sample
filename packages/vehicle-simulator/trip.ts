
/**
 * This object represents a trip
 */
export interface Trip {
  
  /**
   * Unique Id of the trip
   */
  Id: string;

  /**
   * Id of the vehicle driving this trip
   */
  VehicleId: string;

  /**
   * Current duration of the trip
   */
  TripDuration: number;

  /**
   * Desired duration of the trip
   */
  TargetTripDuration: number;

  /**
   * Current status of the trip
   */
  TripStatus: TripStatus;

  /**
   * This helps the simulator distribute the trip load and not do everything once a second
   */
  Ticker: number;
}

export enum TripStatus {
  ACTIVE, FINISHED
}