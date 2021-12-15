import { Trip, TripStatus } from "./trip";
import { v4 as uuid } from 'uuid';
import { EventRecord, EventType } from "./records";
import { Iot, IotData, Firehose } from "aws-sdk";

/**
 * Configures the simulator to create a proper simulation
 */
export interface SimulatorProps {

  /**
   * Name of the Kinesis Firehose Delivery stream used to ingest data
   */
  DeliveryStreamName: string;
  
  /**
   * Desired target number of vehicles in the field.
   */
  TargetVehicles: number;

  /**
   * Desired trip duration, in seconds.
   */
  TargetTripDuration: number;

  /**
   * If this number is set, the simulator will stop after generating the amount of trips set here.
   */
  TargetTripCount?: number;
}

/**
 * This class generates a simulation of driving vehicles based on the configuration given.
 */
export class Simulator {

  private readonly props: SimulatorProps;
  private readonly firehoseClient: Firehose;
  private readonly iotClient: Iot;
  private iotDataClient: IotData;
  
  private finishedTrips = 0;
  private trips: Trip[] = [];
  private simulationStartTime: number = 0;
  private simulationIterationCount: number = 0;

  constructor (props: SimulatorProps) {
    this.props = props;

    this.firehoseClient = new Firehose({
      region: 'eu-west-1'
    });

    this.iotClient = new Iot({
      region: 'eu-west-1'
    });
  }

  async startSimulation () {
    console.log('INFO: Starting vehicle simulation');
    console.log(`INFO: Target details: Vehicle volume (${this.props.TargetVehicles}), Avg Trip duration (${this.props.TargetTripDuration})`);
    console.log('INFO: This process will run until you kill it manually');

    console.log('INFO: Fetching iot endpoint');
    const iotEndpointResponse = await this.iotClient.describeEndpoint({ endpointType: 'iot:Data-ATS' }).promise();
    
    console.log('INFO: Initialising Iot data client');
    this.iotDataClient = new IotData({
      region: 'eu-west-1',
      endpoint: iotEndpointResponse.endpointAddress!
    });

    // Initialize simulation data
    this.trips = [];
    this.simulationStartTime = Date.now();
    this.simulationIterationCount = -1;

    await this.runSimulationIteration();
    setTimeout(() => {
      setInterval(async () => {
        await this.runSimulationIteration();
      }, 90);
    }, 1000);
  }

  async runSimulationIteration () {
    let iterationRecords: EventRecord[] = [];
    this.simulationIterationCount++;
    console.debug(`DEBUG: Starting iteration ${this.simulationIterationCount}`);

    const ticker = this.simulationIterationCount % 10;
    const tickerTrips = this.trips
      .filter(t => t.Ticker === ticker)

    // Create new trips
    let trips = this.trips.filter(t => t.TripStatus === TripStatus.ACTIVE);

    // End trips
    const tripsToFinish = trips.filter(t => t.TripDuration >= t.TargetTripDuration);
    console.debug(`DEBUG: Ending ${tripsToFinish.length} trips`);
    iterationRecords = iterationRecords.concat(tripsToFinish.map(trip => {
      console.debug(`DEBUG: Killing the engine for trip ${trip.Id} (Vehicle (${trip.VehicleId}))`);
      trip.TripStatus = TripStatus.FINISHED;

      return {
        EventId: uuid(),
        TripId: trip.Id,
        DeviceId: trip.VehicleId,
        EventTime: Date.now(),
        EventDate: new Date().toISOString(),
        EventType: EventType.TRIP_FINISHED,

        // If you want the sample to include random data, modify this field
        RandomData: undefined 

        // Add other properties to your records here
        // You will need to modify `records.ts` to reflect the updates.
      }
    }));

    // Submit trip finished event
    tripsToFinish.forEach(trip => {

      // The submission of the event is delayed 90 seconds to wait for all records to be processed
      setTimeout(async () => {
        console.debug(`DEBUG: Notifying system for trip ending. Trip ID: ${trip.Id}`);
        await this.iotDataClient.publish({
          topic: `TripAggregationSample/trips/${trip.Id}/TRIP_FINISHED`,
          payload: JSON.stringify(trip),
          qos: 1
        }).promise();

        console.debug(`DEBUG: Trip ${trip.Id} should start aggregating soon`);
      }, 90000);
    });
    
    this.finishedTrips += tripsToFinish.length;
    this.trips = this.trips.filter(t => t.TripStatus !== TripStatus.FINISHED);

    trips = this.trips;
    const tripCount = trips.length;
    let newTripCount = this.props.TargetVehicles - tripCount;
    if (this.props.TargetTripCount) {
      const availableTrips = this.props.TargetTripCount - this.trips.length - this.finishedTrips;
      newTripCount = Math.min(availableTrips, newTripCount);
    }

    if (newTripCount > 0) {
      console.debug(`DEBUG: Populating the roads with ${newTripCount} new vehicles`);

      const newTrips: Trip[] = new Array(newTripCount).fill(null).map((_, index) => ({
        Id: uuid(),
        VehicleId: uuid(),
        TripDuration: 0,
        TargetTripDuration: this.props.TargetTripDuration * (1 - (Math.random() * 0.3 - 0.15)),
        TripStatus: TripStatus.ACTIVE,
        Ticker: Math.round(Math.random() * 10)
      }));

      this.trips = this.trips.concat(newTrips);
    }

    // Start the ignition on new trips
    const newTrips: Trip[] = tickerTrips.filter(t => t.TripDuration === 0);
    iterationRecords = iterationRecords.concat(newTrips.map(trip => {
      // console.debug(`DEBUG: Starting the engine for trip ${trip.Id}. Trip will last ${trip.TargetTripDuration} seconds`);

      return {
        EventId: uuid(),
        TripId: trip.Id,
        DeviceId: trip.VehicleId,
        EventTime: Date.now(),
        EventDate: new Date().toISOString(),
        EventType: EventType.ENGINE_START,

        // Add any random data to your events
        RandomData: undefined 
      }
    }));

    // Send periodic event on ongoing trips
    const activeTrips = tickerTrips.filter(t => t.TripStatus === TripStatus.ACTIVE);
    console.debug(`DEBUG: There are ${activeTrips.length} vehicles in the field`);
    iterationRecords = iterationRecords.concat(activeTrips.map(trip => {
      trip.TripDuration++;

      return {
        EventId: uuid(),
        TripId: trip.Id,
        DeviceId: trip.VehicleId,
        EventTime: Date.now(),
        EventDate: new Date().toISOString(),
        EventType: EventType.KEEP_ALIVE,

        // Add any random data to your events
        RandomData: undefined 
      }
    }));

    // Publish records
    const recordChunks: any[] = [];

    // Create batch chunks
    for (let i = 0; i < iterationRecords.length; i += 500) {
      recordChunks.push(iterationRecords.slice(i, Math.min(iterationRecords.length, i + 500)));
    }

    console.log(`INFO: There are ${recordChunks.length} data chunks to ingest`);
    await Promise.all(recordChunks.map(async chunk => {
      const ingestionResult = await this.firehoseClient.putRecordBatch({
        DeliveryStreamName: this.props.DeliveryStreamName,
        Records: chunk.map((item: any) => ({
          Data: Buffer.from(JSON.stringify(item))
        }))
      }).promise();

      console.log(`INFO: Ingestion result: ${ingestionResult.RequestResponses.length} records, ${ingestionResult.FailedPutCount} failed`);
    }));

    if (this.props.TargetTripCount && tickerTrips.filter(t => t.TripStatus === TripStatus.FINISHED).length >= this.props.TargetTripCount) {
      console.log(`INFO: Simulation will finish now as target trip count (${this.props.TargetTripCount}) has been reached. ${this.trips.length + this.finishedTrips} trips generated`);
      const processEndDate = Date.now();
      const timeSpent = processEndDate - this.simulationStartTime;
      console.log(`Simulation took ~${timeSpent/1000 | 0} seconds`);
      process.exit(0);
    }
  }
}