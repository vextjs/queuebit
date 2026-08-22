import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';

test('published Queuebit tarball installs and works in a vextjs consumer project', async () => {
  const root = await mkdtemp(join(tmpdir(), 'queuebit-vext-consumer-'));
  try {
    await mkdir(join(root, 'src', 'plugins'), { recursive: true });
    const packageRoot = resolve('.');
    const packageManifest = JSON.parse(await readFile(join(packageRoot, 'package.json'), 'utf8'));
    const vextVersion = packageManifest.peerDependencies?.vextjs;
    assert.equal(typeof vextVersion, 'string');
    await writeFile(join(root, 'package.json'), JSON.stringify({
      name: 'queuebit-vext-consumer-smoke',
      type: 'module',
      private: true
    }, null, 2));
    const packed = runNpm(['pack', '--ignore-scripts', '--json', '--pack-destination', root], packageRoot);
    const packResult = JSON.parse(packed.stdout);
    assert.equal(packResult.length, 1);
    assert.equal(packResult[0].name, packageManifest.name);
    assert.equal(packResult[0].version, packageManifest.version);
    const tarball = join(root, packResult[0].filename);

    runNpm([
      'install',
      '--ignore-scripts',
      '--no-audit',
      '--no-fund',
      '--prefer-offline',
      tarball,
      `vextjs@${vextVersion}`
    ], root);
    const installedManifest = JSON.parse(await readFile(join(root, 'node_modules', 'queuebit', 'package.json'), 'utf8'));
    assert.equal(installedManifest.version, packageManifest.version);
    assert.deepEqual(installedManifest.exports, packageManifest.exports);
    assert.deepEqual(installedManifest.bin, packageManifest.bin);

    runNode([
      '--input-type=module',
      '--eval',
      "import { defineQueuebitConfig } from 'queuebit'; const config = defineQueuebitConfig({ namespace: 'consumer:esm' }); if (config.namespace !== 'consumer:esm') process.exit(1);"
    ], root);
    runNode([
      '--eval',
      "const { defineQueuebitConfig } = require('queuebit'); const config = defineQueuebitConfig({ namespace: 'consumer:cjs' }); if (config.namespace !== 'consumer:cjs') process.exit(1);"
    ], root);
    const bin = process.platform === 'win32'
      ? join(root, 'node_modules', '.bin', 'queuebit.cmd')
      : join(root, 'node_modules', '.bin', 'queuebit');
    const cli = spawnSync(bin, ['--help'], {
      cwd: root,
      encoding: 'utf8',
      shell: process.platform === 'win32'
    });
    assert.equal(cli.status, 0, `${cli.stdout}\n${cli.stderr}\n${cli.error ?? ''}`);
    assert.match(cli.stdout, /queuebit/i);
    await writeFile(join(root, 'tsconfig.json'), JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        strict: true,
        exactOptionalPropertyTypes: true,
        skipLibCheck: true,
        noEmit: true
      },
      include: ['src/**/*.ts']
    }, null, 2));
    await writeFile(join(root, 'src', 'plugins', 'queuebit.ts'), `
import { defineAppExtensions } from 'vextjs';
import { defineQueuebitConfig } from 'queuebit';
import {
  createQueuebitVextPlugin,
  type QueuebitVextAppExtensions
} from 'queuebit/vext';

const config = defineQueuebitConfig({
  namespace: 'consumer:vext',
  queues: {
    notification: {}
  }
});

export const appExtensions = defineAppExtensions<QueuebitVextAppExtensions>();

export default createQueuebitVextPlugin({
  config,
  clientOptions: {
    preflight: false
  }
});
`);

    runNode([join('node_modules', 'typescript', 'bin', 'tsc'), '--project', root], root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

function runNpm(args, cwd) {
  const executable = process.platform === 'win32' ? 'npm.cmd' : 'npm';
  const result = spawnSync(executable, args, {
    cwd,
    encoding: 'utf8',
    timeout: 120_000,
    shell: process.platform === 'win32'
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}\n${result.error ?? ''}`);
  return result;
}

function runNode(args, cwd) {
  const result = spawnSync(process.execPath, args, { cwd, encoding: 'utf8', timeout: 120_000 });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}\n${result.error ?? ''}`);
  return result;
}
