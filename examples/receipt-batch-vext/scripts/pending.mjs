const command = process.argv[2] ?? 'unknown';

console.error(`[queuebit example] ${command} is a v0.1 target-contract script.`);
console.error('The clean-environment receipt-batch-vext example is not runnable evidence in this unreleased checkout.');
console.error('Use docs/v01 and the checked-in queuebit.config.ts / queuebit.runtime.ts as the implementation contract until the release gate closes.');
process.exitCode = 1;
