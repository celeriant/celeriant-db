#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { Ec2KafkaClusterStack } from '../lib/ec2-kafka-cluster-stack';

const app = new cdk.App();
new Ec2KafkaClusterStack(app, 'KafkaBenchmarkStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
