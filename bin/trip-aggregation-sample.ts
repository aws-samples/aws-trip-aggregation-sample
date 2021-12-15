#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { TripAggregationSampleStack } from '../lib/trip-aggregation-sample-stack';

const app = new cdk.App();
new TripAggregationSampleStack(app, 'TripAggregationSample', {});