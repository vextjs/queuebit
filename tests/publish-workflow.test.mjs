import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';

test('publish workflow keeps all quality and Redis release gates ahead of npm publish', async () => {
  const workflow = await readFile(resolve('.github/workflows/publish.yml'), 'utf8');
  const topology = await readFile(resolve('.github/redis/compose.yml'), 'utf8');
  assert.match(workflow, /runs-on: ubuntu-latest/);
  assert.match(workflow, /QUEUEBIT_REDIS_SENTINEL_ALLOW_FAILOVER=1/);
  assert.match(workflow, /docker compose -f \.github\/redis\/compose\.yml down --volumes --remove-orphans/);
  assert.match(topology, /npm run test:redis && npm run test:redis:faults && npm run test:redis:sentinel/);
  assert.match(topology, /cp \/seed\/sentinel\.conf \/tmp\/sentinel\.conf/);
  assert.doesNotMatch(topology, /:\/etc\/redis\/sentinel\.conf:ro/);

  const orderedGates = [
    'npm run typecheck',
    'npm test',
    'npm run docs:validate',
    'npm audit --audit-level=low',
    'npm pack --dry-run --json',
    'docker compose -f .github/redis/compose.yml config -q',
    'docker compose -f .github/redis/compose.yml up -d --wait redis-primary redis-replica sentinel-a sentinel-b',
    'runner'
  ];
  let previous = -1;
  for (const gate of orderedGates) {
    const position = workflow.indexOf(gate);
    assert.notEqual(position, -1, `publish workflow is missing release gate: ${gate}`);
    assert.ok(position > previous, `release gate is out of order: ${gate}`);
    previous = position;
  }

  const publish = workflow.indexOf('npm publish --provenance --access public');
  assert.notEqual(publish, -1, 'publish workflow is missing npm publish');
  assert.ok(publish > previous, 'npm publish must remain after every release gate');
});
