#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { Ec2ClusterStack } from '../lib/ec2-cluster-stack';

const app = new cdk.App();
new Ec2ClusterStack(app, 'CeleriantKtlsTestStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
