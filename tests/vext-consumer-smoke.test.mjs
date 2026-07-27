import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cp, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';

test('queuebit/vext package subpath compiles in a vextjs consumer plugin', async () => {
  const root = await mkdtemp(join(tmpdir(), 'queuebit-vext-consumer-'));
  try {
    await mkdir(join(root, 'src', 'plugins'), { recursive: true });
    await mkdir(join(root, 'node_modules'), { recursive: true });
    await createQueuebitPackageCandidate(root);
    await cp(
      resolve('node_modules/vextjs'),
      join(root, 'node_modules', 'vextjs'),
      { recursive: true }
    );
    await writeFile(join(root, 'package.json'), JSON.stringify({
      name: 'queuebit-vext-consumer-smoke',
      type: 'module',
      private: true,
      dependencies: {
        queuebit: '0.0.3',
        vextjs: '0.3.26'
      }
    }, null, 2));
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

    const tsc = spawnSync(
      process.execPath,
      [resolve('node_modules/typescript/bin/tsc'), '--project', root],
      { cwd: root, encoding: 'utf8' }
    );
    assert.equal(tsc.status, 0, `${tsc.stdout}\n${tsc.stderr}`);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

async function createQueuebitPackageCandidate(root) {
  const packageRoot = join(root, 'node_modules', 'queuebit');
  await mkdir(packageRoot, { recursive: true });
  await cp(resolve('dist'), join(packageRoot, 'dist'), { recursive: true });
  await writeFile(join(packageRoot, 'package.json'), JSON.stringify({
    name: 'queuebit',
    version: '0.0.3',
    type: 'module',
    main: './dist/index.cjs',
    module: './dist/index.js',
    types: './dist/index.d.ts',
    exports: {
      '.': {
        types: './dist/index.d.ts',
        import: './dist/index.js',
        require: './dist/index.cjs'
      },
      './vext': {
        types: './dist/vext/index.d.ts',
        import: './dist/vext/index.js',
        require: './dist/vext/index.cjs'
      },
      './package.json': './package.json'
    },
    peerDependencies: {
      vextjs: '0.3.26'
    },
    peerDependenciesMeta: {
      vextjs: {
        optional: true
      }
    }
  }, null, 2));
}
