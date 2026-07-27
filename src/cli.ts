#!/usr/bin/env node
import { runQueuebitCli } from './cli/index';

runQueuebitCli()
  .then(exitCode => {
    process.exitCode = exitCode;
  })
  .catch(cause => {
    process.stderr.write(`Queuebit CLI crashed: ${cause instanceof Error ? cause.message : String(cause)}\n`);
    process.exitCode = 1;
  });
