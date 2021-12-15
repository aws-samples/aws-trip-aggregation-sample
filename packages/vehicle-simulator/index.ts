#!/usr/bin/env node

const Simulator = require('./simulator').Simulator;

console.log('INFO: Starting simulation engine');
const TargetVehicles = 100;
const TargetTripDuration = 1800;

const simulator = new Simulator({
  TargetTripDuration,
  TargetVehicles,
  DeliveryStreamName: 'TripAggregationDataIngestion_TelemetryIngestion',
});

simulator.startSimulation();

export * from './records';
export * from './simulator';
export * from './trip';
