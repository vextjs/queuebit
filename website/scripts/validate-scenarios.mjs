import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const websiteDir = path.resolve(scriptDir, '..');
const repoDir = path.resolve(websiteDir, '..');
const docsRoot = path.join(repoDir, 'docs');
const docsDir = path.join(docsRoot, 'v01');
const manifest = JSON.parse(
  await readFile(path.join(websiteDir, 'scenarios.json'), 'utf8')
);

const errors = [];
const sources = new Map();

async function readRepoFile(relativePath) {
  return readFile(path.join(repoDir, relativePath), 'utf8');
}

function collectImplementedErrorCodes(source) {
  const union = extractStringUnion(source, 'QueuebitErrorCode');
  return new Set(union);
}

function collectPublicErrorCodeReferences(source) {
  return [...source.matchAll(/\bQB_[A-Z0-9_]+\b/g)]
    .map(match => match[0])
    .filter(code => !code.endsWith('_'));
}

function assertContainsAll(source, label, tokens) {
  for (const token of tokens) {
    if (!source.includes(token)) errors.push(`${label}: missing ${token}`);
  }
}

function assertContainsNone(source, label, tokens) {
  for (const token of tokens) {
    if (source.includes(token)) errors.push(`${label}: forbidden stale token ${token}`);
  }
}

function assertRemoteDrainCommandsDoNotUseTimeout(source, label) {
  const codeBlocks = [...source.matchAll(/```[^\r\n]*\r?\n([\s\S]*?)```/g)]
    .map(match => match[1].replace(/\\\r?\n\s*/g, ' '));
  for (const block of codeBlocks) {
    for (const match of block.matchAll(/\bnpx queuebit (?:worker|coordinator) drain\b[^\r\n]*/g)) {
      const command = match[0];
      if (command.includes('--drain-timeout-ms')) {
        errors.push(`${label}: remote drain command must not use --drain-timeout-ms`);
      }
    }
  }
}

async function listMarkdownFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries.map(async entry => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return listMarkdownFiles(target);
    return entry.isFile() && entry.name.endsWith('.md') ? [target] : [];
  }));
  return nested.flat();
}

function extractStringUnion(source, typeName) {
  const match = source.match(new RegExp(`type ${typeName} =([\\s\\S]*?);`));
  return match ? [...match[1].matchAll(/'([^']+)'/g)].map(item => item[1]) : [];
}

function sameMembers(actual, expected) {
  return actual.length === expected.length
    && actual.every((value, index) => value === expected[index]);
}

function collectPublicApiErrors(source, label) {
  const issues = [];
  const requiredTokens = [
    'type RunStartResult<',
    'deduplicated: boolean;',
    'type QueuebitSerializedError =',
    'failedReason?: QueuebitSerializedError;',
    'pauseRequestedAt?: string;',
    'cancelledAt?: string;',
    'type CompletionEventSummary =',
    'type CompletionSnapshot<',
    'Promise<CursorPage<CompletionEventSummary>>',
    'Promise<CompletionSnapshot<Summary> | null>',
    'jitter?: number;',
    'Promise<RunStartResult<Input>>',
    'type HealthSnapshot =',
    'type QueuebitMetricSample =',
    'interface HealthApi',
    'interface MetricsApi'
  ];
  for (const token of requiredTokens) {
    if (!source.includes(token)) issues.push(`${label}: missing ${token}`);
  }

  for (const forbidden of [
    'type CompletionEvent =',
    'Promise<CursorPage<CompletionEventSnapshot>>',
    'blockedReason?:',
    'lastError?: QueuebitError',
    'defineBatchSource',
    'defineBatchMapper',
    'defineCompletionHandler',
    'defineJobProcessor',
    'lifecycle:'
  ]) {
    if (source.includes(forbidden)) {
      issues.push(`${label}: forbidden stale token ${forbidden}`);
    }
  }

  const expectedUnions = {
    JobState: [
      'waiting', 'active', 'delayed', 'retrying',
      'completed', 'failed', 'cancelled'
    ],
    RunExecutionState: [
      'created', 'running', 'pausing', 'paused', 'blocked', 'cancelling',
      'completed', 'partial_failed', 'failed', 'cancelled'
    ],
    CompletionState: [
      'not_created', 'not_required', 'pending', 'delivering',
      'retrying', 'delivered', 'failed'
    ]
  };
  for (const [typeName, expected] of Object.entries(expectedUnions)) {
    const actual = extractStringUnion(source, typeName);
    if (!sameMembers(actual, expected)) {
      issues.push(
        `${label}: ${typeName} expected ${expected.join('|')}, received ${actual.join('|')}`
      );
    }
  }
  return issues;
}

function collectCodeFenceLanguages(source) {
  return [...source.matchAll(/```([a-zA-Z0-9_-]*)/g)]
    .map(match => match[1] || 'plain');
}

function assertNegativeProbe(name, mutatedSource, expectedText) {
  const issues = collectPublicApiErrors(mutatedSource, `negative:${name}`);
  if (!issues.some(issue => issue.includes(expectedText))) {
    errors.push(`negative probe ${name} did not detect ${expectedText}`);
  }
}

if (manifest.contractStatus !== 'confirmed') {
  errors.push(
    `scenario contract is ${manifest.contractStatus ?? 'unspecified'}: ${manifest.invalidatedReason ?? 'CP1 confirmation is required'}`
  );
}
if (manifest.evidenceLevel !== 'target-contract') {
  errors.push(`expected target-contract evidence level, received ${manifest.evidenceLevel ?? 'unspecified'}`);
}

async function readDoc(language, file) {
  const key = `${language}/${file}`;
  if (!sources.has(key)) {
    sources.set(key, await readFile(path.join(docsDir, language, file), 'utf8'));
  }
  return sources.get(key);
}

for (const page of manifest.requiredPages) {
  for (const field of ['role', 'audience', 'sidebarGroup', 'userPath']) {
    if (page[field] === undefined || page[field] === '') {
      errors.push(`${page.file}: missing page-role field ${field}`);
    }
  }
  for (const language of ['en', 'zh']) {
    try {
      const source = await readDoc(language, page.file);
      if (!source.includes('manual-label')) {
        errors.push(`${language}/${page.file}: missing user-manual role label`);
      }
    } catch (error) {
      errors.push(`${language}/${page.file}: ${error.message}`);
    }
  }
}

for (const scenario of manifest.scenarios) {
  const requiredFields = [
    'inScope', 'trigger', 'config', 'execute', 'expectedState', 'failure',
    'recovery', 'observe', 'executableEvidence', 'status'
  ];
  for (const field of requiredFields) {
    if (scenario[field] === undefined || scenario[field] === '') {
      errors.push(`${scenario.id}: missing scenario coverage field ${field}`);
    }
  }
  if (scenario.status !== 'documented-target') {
    errors.push(`${scenario.id}: target contract must use documented-target status, received ${scenario.status}`);
  }
  for (const language of ['en', 'zh']) {
    try {
      const source = await readDoc(language, scenario.file);
      const marker = `<span id="${scenario.marker}"></span>`;
      if (!source.includes(marker)) {
        errors.push(`${scenario.id} ${language}/${scenario.file}: missing ${marker}`);
      }
    } catch (error) {
      errors.push(`${scenario.id} ${language}/${scenario.file}: ${error.message}`);
    }
  }
}

const config = await readFile(path.join(websiteDir, 'rspress.config.ts'), 'utf8');
for (const page of manifest.requiredPages) {
  if (page.sidebar === false) continue;

  const route = page.file === 'index.md' ? '' : page.file.replace(/\.md$/, '');
  const englishLink = route ? `/${route}'` : "link: '/'";
  const chineseLink = route ? `/zh/${route}'` : "link: '/zh/'";
  if (!config.includes(englishLink) || !config.includes(chineseLink)) {
    errors.push(`${page.file}: missing English or Chinese sidebar route`);
  }
}

for (const language of ['en', 'zh']) {
  for (const page of manifest.requiredPages) {
    const source = await readDoc(language, page.file);
    if (source.includes('completion_failed')) {
      errors.push(`${language}/${page.file}: use public completion state failed, not completion_failed`);
    }
  }
}

const homeEntryLinks = {
  en: ['/quick-start.html', '/batch-runs.html', '/vext-integration.html'],
  zh: ['/zh/quick-start.html', '/zh/batch-runs.html', '/zh/vext-integration.html']
};
for (const [language, links] of Object.entries(homeEntryLinks)) {
  const home = await readDoc(language, 'index.md');
  for (const link of links) {
    if (!home.includes(link)) {
      errors.push(`${language}/index.md: missing home entry route ${link}`);
    }
  }
}

for (const language of ['en', 'zh']) {
  const source = await readDoc(language, 'batch-runs.md');
  const sequence = [...source.matchAll(/^## (\d+)\./gm)].map(match => Number(match[1]));
  const expected = [1, 2, 3, 4, 5, 6, 7];
  if (sequence.length !== expected.length
    || sequence.some((value, index) => value !== expected[index])) {
    errors.push(`${language}/batch-runs.md: expected numbered H2 sequence 1..7, received ${sequence.join(',')}`);
  }
}

function extractPublicApiCode(source, language) {
  const anchor = '<a id="public-api-contract"></a>';
  const start = source.indexOf(anchor);
  const end = source.indexOf('## Jobs API', start);
  if (start < 0 || end < 0) {
    errors.push(`${language}/target-api.md: missing public API contract section`);
    return '';
  }
  const section = source.slice(start, end);
  const blocks = [...section.matchAll(/```ts\r?\n([\s\S]*?)```/g)]
    .map(match => match[1]);
  if (blocks.length !== 2) {
    errors.push(`${language}/target-api.md: expected two public API TypeScript blocks, received ${blocks.length}`);
  }
  return blocks.join('\n');
}

const englishApiCode = extractPublicApiCode(await readDoc('en', 'target-api.md'), 'en');
const chineseApiCode = extractPublicApiCode(await readDoc('zh', 'target-api.md'), 'zh');
const requiredApiTokens = [
  'type CursorPage<T>',
  'type JobSnapshot<',
  'type RunSnapshot<',
  'type RunStartResult<',
  'type FailureRecord<',
  'type QueuebitSerializedError =',
  'type CompletionEventSummary =',
  'type CompletionSnapshot<',
  'type CompletionEventSnapshot<',
  'type JobBackoffOptions =',
  'type HealthSnapshot =',
  'type QueuebitMetricSample =',
  'interface JobsApi',
  'interface RunsApi',
  'interface CompletionsApi',
  'interface HealthApi',
  'interface MetricsApi',
  'deduplicated: boolean;',
  'jitter?: number;',
  'Promise<RunStartResult<Input>>',
  'Promise<CursorPage<CompletionEventSummary>>',
  'Promise<CompletionSnapshot<Summary> | null>'
];
for (const token of requiredApiTokens) {
  if (!englishApiCode.includes(token) || !chineseApiCode.includes(token)) {
    errors.push(`target-api.md: missing bilingual public contract token ${token}`);
  }
}
if (englishApiCode !== chineseApiCode) {
  errors.push('target-api.md: English and Chinese public TypeScript contracts differ');
}
errors.push(...collectPublicApiErrors(englishApiCode, 'en/target-api.md'));
errors.push(...collectPublicApiErrors(chineseApiCode, 'zh/target-api.md'));

assertNegativeProbe(
  'run-start-result',
  englishApiCode.replace('deduplicated: boolean;', ''),
  'deduplicated: boolean;'
);
assertNegativeProbe(
  'job-backoff-jitter',
  englishApiCode.replace('jitter?: number;', ''),
  'jitter?: number;'
);
assertNegativeProbe(
  'public-stalled-state',
  englishApiCode.replace(
    "| 'completed' | 'failed' | 'cancelled';",
    "| 'stalled' | 'completed' | 'failed' | 'cancelled';"
  ),
  'JobState expected'
);

for (const language of ['en', 'zh']) {
  const quickStart = await readDoc(language, 'quick-start.md');
  for (const token of [
    'queuebit.jobs.add(',
    'createQueuebitClient(queuebitConfig)',
    'queuebit.createWorker(',
    'worker.start()',
    'queuebit.close('
  ]) {
    if (!quickStart.includes(token)) {
      errors.push(`${language}/quick-start.md: missing first-job token ${token}`);
    }
  }
  assertContainsNone(quickStart, `${language}/quick-start.md`, [
    'class="qb-canonical-flow"',
    "queuebit.runs.start('receipt-campaign'",
    'target-contract skeleton',
    'completionState=not_created',
    '"deduplicated": false',
    'blockedReason',
    'lastError',
    'title="queuebit.runtime.ts"',
    'npx queuebit job inspect <jobId>'
  ]);
  if (quickStart.toLowerCase().includes('docker')) {
    errors.push(`${language}/quick-start.md: Docker instructions belong outside Quick Start`);
  }
  if (/^\s*namespace:\s*/m.test(quickStart)) {
    errors.push(`${language}/quick-start.md: use the automatic application namespace in Quick Start`);
  }

  const batchRun = await readDoc(language, 'batch-runs.md');
  const stageCount = (batchRun.match(/class="qb-flow-stage(?: qb-flow-stage--final)?"/g) ?? []).length;
  if (!batchRun.includes('class="qb-canonical-flow"')
    || !batchRun.includes('role="img"')
    || stageCount !== 5) {
    errors.push(`${language}/batch-runs.md: canonical flow must expose one labelled visual with five stages`);
  }
  if (!batchRun.includes('target-contract skeleton')) {
    errors.push(`${language}/batch-runs.md: missing unreleased example boundary`);
  }
  for (const token of [
    'ReceiptRepository',
    'startReceiptWorker',
    'startReceiptCoordinator',
    'CoordinatorRunner',
    'status().lastError',
    'onError',
    'AuthenticatedReceiptActor',
    'actor.tenantId',
    "queuebit.runs.start('receipt-campaign'",
    'idempotencyKey'
  ]) {
    if (!batchRun.includes(token)) {
      errors.push(`${language}/batch-runs.md: missing current code-first truth token ${token}`);
    }
  }
  assertContainsNone(batchRun, `${language}/batch-runs.md`, [
    'blockedReason',
    'app.queuebit.runs.start',
    "tenantId: 'tenant-42'"
  ]);
}

const quickStartCodeFencesEn = collectCodeFenceLanguages(await readDoc('en', 'quick-start.md'));
const quickStartCodeFencesZh = collectCodeFenceLanguages(await readDoc('zh', 'quick-start.md'));
if (!sameMembers(quickStartCodeFencesEn, quickStartCodeFencesZh)) {
  errors.push('quick-start.md: English and Chinese code fence language sequences differ');
}

const accessibilitySource = await readFile(
  path.join(websiteDir, 'components', 'A11yLabels.tsx'),
  'utf8'
);
assertContainsAll(accessibilitySource, 'A11yLabels.tsx release banner', [
  'useLang()',
  "title: 'v0.1 预览文档'",
  "title: 'v0.1 preview documentation'",
  'className="qb-release-banner"',
  'role="status"',
  'aria-live="polite"'
]);
assertContainsAll(accessibilitySource, 'A11yLabels.tsx mobile documentation sidebar', [
  'useEffect(',
  "window.matchMedia('(max-width: 768px)')",
  "sidebarSelector = '.rp-doc-layout__sidebar'",
  "openClass = 'rp-doc-layout__sidebar--open'",
  "event.target.closest('.rp-sidebar-menu__left')",
  "document.addEventListener('click', handleDocumentClick, true)",
  "menuButton.setAttribute('aria-controls', sidebar.id)",
  "menuButton.setAttribute('aria-expanded', String(mobileSidebarOpen))",
  'className="qb-mobile-sidebar-mask"',
  'aria-label={copy.closeMenu}'
]);
assertContainsNone(accessibilitySource, 'A11yLabels.tsx unsupported global mutation', [
  'MutationObserver',
  'document.body'
]);

const styles = await readFile(path.join(websiteDir, 'styles', 'queuebit.css'), 'utf8');
for (const token of [
  '.qb-canonical-flow',
  '.qb-flow-stage',
  '.qb-flow-arrow',
  '.qb-mobile-sidebar-mask',
  '.rp-doc-layout__sidebar.rp-doc-layout__sidebar--open'
]) {
  if (!styles.includes(token)) {
    errors.push(`queuebit.css: missing canonical flow style ${token}`);
  }
}

function collectReadmeBoundaryErrors(source) {
  const issues = [];
  const links = [...source.matchAll(/\]\(([^)]*docs\/v01\/[^)]+)\)/g)]
    .map(match => match[1]);
  if (links.length === 0) issues.push('README: no docs/v01 links');
  for (const link of links) {
    if (!link.startsWith('https://github.com/devcodex-labs/queuebit/blob/')) {
      issues.push(`README: non-publishable docs link ${link}`);
    }
  }
  if (!source.includes('Redis requires `>=7.2`')) {
    issues.push('README: Redis baseline is not >=7.2');
  }
  if (!source.includes('Node.js `>=20`')
    || !source.includes('Node.js `>=20.19`')) {
    issues.push('README: core and vext Node baselines are not separated');
  }
  for (const token of [
    'npm run docs:preview',
    'http://localhost:4180/queuebit/',
    'http://localhost:4180/queuebit/zh/',
    'npm run docs:dev',
    '127.0.0.1:4181',
    'npm run docs:edit',
    '127.0.0.1:4182'
  ]) {
    if (!source.includes(token)) {
      issues.push(`README: missing stable local docs token ${token}`);
    }
  }
  return issues;
}

const readme = await readFile(path.join(repoDir, 'README.md'), 'utf8');
errors.push(...collectReadmeBoundaryErrors(readme));
const relativeReadme = readme.replace(
  'https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/quick-start.md',
  'docs/v01/en/quick-start.md'
);
if (!collectReadmeBoundaryErrors(relativeReadme)
  .some(issue => issue.includes('non-publishable docs link'))) {
  errors.push('negative probe README relative-link did not fail');
}

const packageMetadata = JSON.parse(
  await readFile(path.join(repoDir, 'package.json'), 'utf8')
);
const websitePackageMetadata = JSON.parse(
  await readFile(path.join(websiteDir, 'package.json'), 'utf8')
);
const packageFiles = [...(packageMetadata.files ?? [])].sort();
if (!sameMembers(packageFiles, ['LICENSE', 'README.md', 'dist'])) {
  errors.push(`package.json: expected files LICENSE|README.md|dist, received ${packageFiles.join('|')}`);
}
if (packageMetadata.scripts?.['docs:preview']
  !== 'npm run docs:build && npm --prefix website run preview') {
  errors.push('package.json: docs:preview must build then serve the fixed preview port');
}
if (packageMetadata.scripts?.['docs:dev']
  !== 'npm run docs:build && npm --prefix website run dev') {
  errors.push('package.json: docs:dev must build then delegate to the fixed 4181 generated preview script');
}
if (packageMetadata.scripts?.['docs:edit'] !== 'npm --prefix website run edit') {
  errors.push('package.json: docs:edit must delegate to the fixed 4182 hot-edit script');
}
if (websitePackageMetadata.scripts?.preview
  !== 'rspress preview --port 4180 --host 127.0.0.1') {
  errors.push('website/package.json: preview must pin 127.0.0.1:4180');
}
if (websitePackageMetadata.scripts?.dev
  !== 'rspress preview --port 4181 --host 127.0.0.1') {
  errors.push('website/package.json: dev must pin generated preview to 127.0.0.1:4181');
}
if (websitePackageMetadata.scripts?.edit
  !== 'rspress dev --port 4182 --host 127.0.0.1') {
  errors.push('website/package.json: edit must pin hot dev to 127.0.0.1:4182');
}

const legacyFiles = (
  await Promise.all(['en', 'zh'].map(language =>
    listMarkdownFiles(path.join(docsRoot, language))
  ))
).flat();
if (legacyFiles.length === 0) errors.push('legacy docs: no archived pages found');
const legacyMarker = '<!-- queuebit-v01-legacy-doc -->';
const legacyPages = await Promise.all(legacyFiles.map(async file => ({
  file: path.relative(repoDir, file),
  source: await readFile(file, 'utf8')
})));
function collectLegacyArchiveErrors(pages) {
  const issues = [];
  for (const page of pages) {
    if (!page.source.includes(legacyMarker)) {
      issues.push(`${page.file}: missing legacy archive marker`);
    }
  }
  return issues;
}
errors.push(...collectLegacyArchiveErrors(legacyPages));
if (legacyFiles.length > 0) {
  const mutatedLegacyPages = legacyPages.map((page, index) => index === 0
    ? { ...page, source: page.source.replace(legacyMarker, '') }
    : page);
  if (!collectLegacyArchiveErrors(mutatedLegacyPages)
    .some(issue => issue.includes('missing legacy archive marker'))) {
    errors.push('negative probe legacy marker removal did not fail');
  }
}

const baselineFiles = [
  ['README.md', readme],
  ['en/compatibility.md', await readDoc('en', 'compatibility.md')],
  ['zh/compatibility.md', await readDoc('zh', 'compatibility.md')],
  ['en/production-deployment.md', await readDoc('en', 'production-deployment.md')],
  ['zh/production-deployment.md', await readDoc('zh', 'production-deployment.md')]
];
for (const [file, source] of baselineFiles) {
  if (!source.includes('>=7.2') || source.includes('>=7.0')) {
    errors.push(`${file}: Redis baseline must be >=7.2 with no >=7.0 promise`);
  }
}

for (const language of ['en', 'zh']) {
  const batchRun = await readDoc(language, 'batch-runs.md');
  for (const token of [
    '"deduplicated": false',
    '"executionState": "created"',
    '"completionState": "not_created"',
    'executionState=running',
    'completionState=not_created'
  ]) {
    if (!batchRun.includes(token)) {
      errors.push(`${language}/batch-runs.md: missing Run start/state token ${token}`);
    }
  }
  if (/executionState=running\r?\ncompletionState=pending/.test(batchRun)) {
    errors.push(`${language}/batch-runs.md: in-flight Run cannot have pending completion`);
  }

  const jobRecipes = await readDoc(language, 'job-recipes.md');
  if (!jobRecipes.includes('jitter: 0.2')) {
    errors.push(`${language}/job-recipes.md: canonical retry recipe must exercise jitter`);
  }

  const vext = await readDoc(language, 'vext-integration.md');
  if (!vext.includes('run.deduplicated')) {
    errors.push(`${language}/vext-integration.md: canonical adapter must expose deduplicated`);
  }
  for (const token of [
    'defineAppExtensions',
    'createQueuebitVextPlugin',
    'app.queuebit.runs.start',
    'clientOptions.redis'
  ]) {
    if (!vext.includes(token)) {
      errors.push(`${language}/vext-integration.md: missing real vext adapter token ${token}`);
    }
  }
  for (const forbidden of [
    "import { queuebit } from 'queuebit/vext'",
    'useQueuebit',
    'createVextQueueWorker',
    'createVextQueueScheduler'
  ]) {
    if (vext.includes(forbidden)) {
      errors.push(`${language}/vext-integration.md: leaked unpublished vext helper ${forbidden}`);
    }
  }

  const cli = await readDoc(language, 'cli-reference.md');
  if (/^npx queuebit scheduler (?:start|inspect|drain)\b/m.test(cli)) {
    errors.push(`${language}/cli-reference.md: standalone Scheduler command leaked into v0.1`);
  }
  for (const token of ['QB_CLI_COMMAND_UNSUPPORTED', 'exit code 2']) {
    if (!cli.includes(token)) {
      errors.push(`${language}/cli-reference.md: missing Scheduler rejection token ${token}`);
    }
  }
  assertContainsAll(cli, `${language}/cli-reference.md: code-first runtime contract`, [
    'createQueuebitRuntimeProcessor',
    'createCoordinatorRunner',
    'queuebit.close({ timeoutMs: 60_000 })',
    'npx queuebit worker start',
    'npx queuebit coordinator start',
    'TENANT_ID=',
    'PAID_BEFORE=',
    '${TENANT_ID}',
    '${PAID_BEFORE}'
  ]);
  assertContainsNone(cli, `${language}/cli-reference.md: hard-coded business identity`, [
    'tenant-42',
    'tenant-demo'
  ]);
  assertRemoteDrainCommandsDoNotUseTimeout(cli, `${language}/cli-reference.md`);

  const targetApi = await readDoc(language, 'target-api.md');
  const targetIdentityPhrase = language === 'zh' ? 'Queuebit 不会' : 'Queuebit does not';
  assertContainsAll(targetApi, `${language}/target-api.md: server-derived Run identity`, [
    'actor.tenantId',
    'request.paidBefore',
    targetIdentityPhrase
  ]);
  assertContainsNone(targetApi, `${language}/target-api.md: hard-coded business identity`, [
    'tenant-42',
    'tenant-demo'
  ]);

  const redisModel = await readDoc(language, 'redis-model.md');
  for (const token of ['SCAN qb:{namespace}:*', 'FLUSHDB', 'FLUSHALL', 'SCRIPT FLUSH']) {
    if (!redisModel.includes(token)) {
      errors.push(`${language}/redis-model.md: missing Redis cleanup boundary token ${token}`);
    }
  }

  const operations = await readDoc(language, 'operations.md');
  assertRemoteDrainCommandsDoNotUseTimeout(operations, `${language}/operations.md`);
  for (const token of ['capacity.snapshot()', 'completionEvents.ageMs/maxCount']) {
    if (!operations.includes(token)) {
      errors.push(`${language}/operations.md: missing operations source truth token ${token}`);
    }
  }

  const distributedWorkers = await readDoc(language, 'distributed-workers.md');
  assertRemoteDrainCommandsDoNotUseTimeout(distributedWorkers, `${language}/distributed-workers.md`);
  if (!distributedWorkers.includes('--include-stale')) {
    errors.push(`${language}/distributed-workers.md: missing stale role inspection flag`);
  }

  const workerLifecycle = await readDoc(language, 'worker-lifecycle.md');
  for (const token of [
    'leaseGeneration',
    'complete(jobId, leaseGeneration',
    'fail(jobId, leaseGeneration',
    'QB_JOB_STATE_CONFLICT'
  ]) {
    if (!workerLifecycle.includes(token)) {
      errors.push(`${language}/worker-lifecycle.md: missing Worker lifecycle source truth token ${token}`);
    }
  }

  const configContract = await readDoc(language, 'cli-and-config.md');
  for (const token of [
    'backgroundReconnect.initialDelayMs',
    'backgroundReconnect.maxDelayMs',
    'backgroundReconnect.factor',
    'backgroundReconnect.jitter',
    'backgroundReconnect.logThrottleMs',
    'QB_CONFIG_INVALID',
    'QB_RUN_STATE_CONFLICT',
    'completionEvents.ageMs/maxCount',
    '| `concurrency` | 1',
    '| `drainTimeoutMs` | 60000',
    'client.createCoordinatorRunner(runtime, options)',
    '| `completionLimit` | 25',
    'status().lastError',
    'QB_COORDINATOR_DRAIN_TIMEOUT'
  ]) {
    if (!configContract.includes(token)) {
      errors.push(`${language}/cli-and-config.md: missing exact config token ${token}`);
    }
  }
  const pageSizeDefaultToken = language === 'en'
    ? '| `pageSize` | no | 100'
    : '| `pageSize` | 否 | 100';
  if (!configContract.includes(pageSizeDefaultToken)) {
    errors.push(`${language}/cli-and-config.md: missing pageSize default`);
  }
  const attemptsDefaultToken = language === 'en'
    ? 'default `attempts` value is `3`'
    : '默认 `attempts` 是 `3`';
  if (!configContract.includes(attemptsDefaultToken)) {
    errors.push(`${language}/cli-and-config.md: missing completion attempts default`);
  }
  assertContainsNone(configContract, `${language}/cli-and-config.md`, [
    'QB_CONFIG_SCHEDULER_MODE_UNSUPPORTED',
    'QB_CONFIG_SCHEMA_KEYWORD_UNSUPPORTED',
    'failurePolicy',
    'sourceRetry',
    'dispatchRetry',
    'jitter=0.2'
  ]);

  const development = await readDoc(language, 'development-contract.md');
  for (const token of ['M0A', 'M2K', 'release gate', 'hard limit']) {
    if (!development.includes(token)) {
      errors.push(`${language}/development-contract.md: missing acceptance boundary token ${token}`);
    }
  }

  const productionDeployment = await readDoc(language, 'production-deployment.md');
  for (const token of [
    'QUEUEBIT_REDIS_URL',
    'QUEUEBIT_REDIS_HOST',
    'QUEUEBIT_REDIS_SENTINEL_MASTER',
    'QUEUEBIT_REDIS_SENTINELS',
    'startWorkerHost',
    'startCoordinatorHost',
    'createQueuebitRuntimeProcessor',
    'createCoordinatorRunner',
    "logger.error({ event }, 'Queuebit coordinator error')"
  ]) {
    if (!productionDeployment.includes(token)) {
      errors.push(`${language}/production-deployment.md: missing target evidence env token ${token}`);
    }
  }
  assertContainsNone(productionDeployment, `${language}/production-deployment.md`, ['productionLogger']);
}

const runtimeApiSource = await readRepoFile('src/runtime/api.ts');
assertContainsAll(runtimeApiSource, 'src/runtime/api.ts', [
  'export function defineQueuebitRuntime',
  'export function defineQueuebitSource',
  'export function defineQueuebitMapper',
  'export function defineQueuebitCompletionHandler',
  'export function defineQueuebitProcessor',
  'export function createQueuebitRuntimeProcessor'
]);

const coordinatorRunnerSource = await readRepoFile('src/coordinator/runner.ts');
assertContainsAll(coordinatorRunnerSource, 'src/coordinator/runner.ts', [
  'export function createQueuebitCoordinatorRunner',
  "code: 'QB_COORDINATOR_DRAIN_TIMEOUT'",
  '#recordError',
  'onError'
]);
const clientSource = await readRepoFile('src/client.ts');
assertContainsAll(clientSource, 'src/client.ts: code-first role lifecycle', [
  'createCoordinatorRunner(',
  'coordinatorRunners',
  'runner.stop(closeOptions)'
]);

const configSource = await readRepoFile('src/config.ts');
  assertContainsAll(configSource, 'src/config.ts defaults', [
  'concurrency: input.workerDefaults?.concurrency ?? builtInDefaults.workerDefaults.concurrency',
  'input.workerDefaults?.drainTimeoutMs ?? builtInDefaults.workerDefaults.drainTimeoutMs',
  'pageSize: runConfig.pageSize ?? 100',
  'attempts: handler.attempts ?? 3'
]);

const implementedErrorCodes = collectImplementedErrorCodes(await readRepoFile('src/errors.ts'));
const manualFiles = (
  await Promise.all(['en', 'zh'].map(language =>
    listMarkdownFiles(path.join(docsDir, language))
  ))
).flat();
const manualPages = await Promise.all(manualFiles.map(async file => ({
  file: path.relative(repoDir, file),
  source: await readFile(file, 'utf8')
})));
for (const page of manualPages) {
  for (const code of collectPublicErrorCodeReferences(page.source)) {
    if (!implementedErrorCodes.has(code)) {
      errors.push(`${page.file}: references unimplemented error code ${code}`);
    }
  }
  assertContainsNone(page.source, page.file, [
    'defineBatchSource',
    'defineBatchMapper',
    'defineCompletionHandler',
    'defineJobProcessor',
    'upperBound',
    'initialCursor',
    'skipped: true',
    'failurePolicy',
    'sourceRetry',
    'dispatchRetry',
    'QB_CONFIG_REDIS_CLUSTER_UNSUPPORTED',
    'QB_SERVER_POLICY_NOT_READY',
    'QB_LEASE_STALE_ATTEMPT',
    'QB_JOB_TIMEOUT',
    'QB_COMPLETION_RETRY_EXHAUSTED',
    'QB_PAYLOAD_TOO_LARGE',
    'QB_DEDUPLICATION_CONFLICT',
    'QB_REDIS_UNAVAILABLE',
    'QB_RUN_RECOVERY_DATA_EXPIRED',
    'QB_CONFIG_UNKNOWN_FIELD'
  ]);
}

const scenario10 = manifest.scenarios.find(scenario => scenario.id === 'SC-10');
if (!scenario10?.config.includes('Redis >=7.2')) {
  errors.push('SC-10: Redis baseline must stay >=7.2');
}
const scenario8 = manifest.scenarios.find(scenario => scenario.id === 'SC-08');
if (scenario8?.observe.includes('blockedReason')) {
  errors.push('SC-08: observe must not reference non-public Run blockedReason');
}

const examplePackage = JSON.parse(
  await readRepoFile('examples/receipt-batch-vext/package.json')
);
for (const script of [
  'typecheck',
  'infra:up',
  'infra:health',
  'infra:ports',
  'infra:down',
  'db:migrate',
  'seed',
  'start:web',
  'audit:show',
  'stop:roles'
]) {
  if (examplePackage.scripts?.[script] === undefined) {
    errors.push(`examples/receipt-batch-vext/package.json: missing ${script} script`);
  }
}
const exampleTsconfig = await readRepoFile('examples/receipt-batch-vext/tsconfig.json');
assertContainsAll(exampleTsconfig, 'examples/receipt-batch-vext/tsconfig.json', [
  '"extends": "../../tsconfig.json"',
  '"queuebit": ["src/index.ts"]',
  '"queuebit/vext": ["src/vext/index.ts"]'
]);
const exampleReadme = await readRepoFile('examples/receipt-batch-vext/README.md');
assertContainsAll(exampleReadme, 'examples/receipt-batch-vext/README.md', [
  'target-contract',
  'not runnable evidence yet',
  'clean-environment example gate'
]);
const examplePendingScript = await readRepoFile('examples/receipt-batch-vext/scripts/pending.mjs');
assertContainsAll(examplePendingScript, 'examples/receipt-batch-vext/scripts/pending.mjs', [
  'target-contract script',
  'not runnable evidence'
]);
const exampleConfig = await readRepoFile('examples/receipt-batch-vext/queuebit.config.ts');
assertContainsAll(exampleConfig, 'examples/receipt-batch-vext/queuebit.config.ts', [
  'defineQueuebitConfig',
  "source: 'paid-orders'",
  "mapper: 'receipt-jobs'",
  'pageSize: 10',
  "handler: 'record-receipt-batch-result'",
  "handler: 'record-receipt-run-result'"
]);
assertContainsNone(exampleConfig, 'examples/receipt-batch-vext/queuebit.config.ts', [
  'jobDefaults',
  'failurePolicy',
  'sourceRetry',
  'dispatchRetry'
]);
const exampleRuntime = await readRepoFile('examples/receipt-batch-vext/queuebit.runtime.ts');
assertContainsAll(exampleRuntime, 'examples/receipt-batch-vext/queuebit.runtime.ts', [
  'defineQueuebitRuntime',
  'defineQueuebitSource',
  'defineQueuebitMapper',
  'defineQueuebitProcessor',
  'defineQueuebitCompletionHandler',
  'boundary: { maxId: boundary.maxId }',
  'return null',
  'identity:',
  'idempotencyKey:'
]);
assertContainsNone(exampleRuntime, 'examples/receipt-batch-vext/queuebit.runtime.ts', [
  'defineBatchSource',
  'defineBatchMapper',
  'defineCompletionHandler',
  'defineJobProcessor',
  'const orders'
]);
const exampleRepository = await readRepoFile('examples/receipt-batch-vext/receipt-repository.ts');
assertContainsAll(exampleRepository, 'examples/receipt-batch-vext/receipt-repository.ts', [
  'freezePaidOrders',
  'loadPaidOrders',
  'sendReceipt',
  'recordReceiptBatchCompletion',
  'recordReceiptRunCompletion'
]);
const exampleServices = await readRepoFile('examples/receipt-batch-vext/receipt-services.ts');
assertContainsAll(exampleServices, 'examples/receipt-batch-vext/receipt-services.ts', [
  'startReceiptWorker',
  'startReceiptCoordinator',
  'createQueuebitRuntimeProcessor',
  'createCoordinatorRunner',
  'client.close(closeOptions)'
]);
const exampleCampaign = await readRepoFile('examples/receipt-batch-vext/start-receipt-campaign.ts');
assertContainsAll(exampleCampaign, 'examples/receipt-batch-vext/start-receipt-campaign.ts', [
  'AuthenticatedReceiptActor',
  'actor.tenantId',
  "queuebit.runs.start('receipt-campaign'",
  'idempotencyKey'
]);

if (errors.length > 0) {
  console.error(`Scenario validation failed with ${errors.length} error(s):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  console.log(
    `Validated ${manifest.scenarios.length} target scenarios, ${manifest.requiredPages.length} bilingual site pages, ${legacyFiles.length} archived legacy pages, and the semantic public-contract regression set.`
  );
}
