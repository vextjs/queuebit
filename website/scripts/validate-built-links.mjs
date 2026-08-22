import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';

const distRoot = path.resolve('dist');
const origin = 'https://queuebit.local';
const htmlFiles = await collectHtmlFiles(distRoot);
const pages = new Map();

for (const file of htmlFiles) {
  const source = await readFile(file, 'utf8');
  pages.set(path.resolve(file), {
    anchors: collectAnchors(source),
    hrefs: collectHrefs(source),
    route: routeForFile(file)
  });
}

const errors = [];
let checkedLinks = 0;
for (const [file, page] of pages) {
  for (const href of page.hrefs) {
    if (shouldIgnoreHref(href)) continue;
    const resolved = new URL(href, `${origin}${page.route}`);
    if (resolved.origin !== origin || !isQueuebitRoute(resolved.pathname)) continue;

    checkedLinks += 1;
    const targetFile = fileForRoute(resolved.pathname);
    const target = pages.get(targetFile);
    if (target === undefined) {
      errors.push(`${displayPath(file)} -> ${href}: target ${displayPath(targetFile)} is missing`);
      continue;
    }
    const anchor = decodeURIComponent(resolved.hash.slice(1));
    if (anchor.length > 0 && !target.anchors.has(anchor)) {
      errors.push(`${displayPath(file)} -> ${href}: anchor #${anchor} is missing`);
    }
  }
}

if (errors.length > 0) {
  console.error(`Built link validation failed with ${errors.length} error(s):`);
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  console.log(`Validated ${htmlFiles.length} built HTML pages and ${checkedLinks} local navigation links.`);
}

async function collectHtmlFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...await collectHtmlFiles(target));
    } else if (entry.isFile() && entry.name.endsWith('.html')) {
      files.push(target);
    }
  }
  return files;
}

function collectAnchors(source) {
  const anchors = new Set();
  for (const match of source.matchAll(/\b(?:id|name)=(['"])(.*?)\1/gi)) {
    anchors.add(match[2]);
  }
  return anchors;
}

function collectHrefs(source) {
  return [...source.matchAll(/<a\b[^>]*\bhref=(['"])(.*?)\1/gi)].map(match => match[2]);
}

function shouldIgnoreHref(href) {
  return href.length === 0 || /^(?:mailto:|tel:|javascript:|data:)/i.test(href);
}

function isQueuebitRoute(pathname) {
  return pathname === '/queuebit' || pathname.startsWith('/queuebit/');
}

function routeForFile(file) {
  const relative = path.relative(distRoot, file).split(path.sep).join('/');
  if (relative === 'index.html') return '/queuebit/';
  if (relative.endsWith('/index.html')) return `/queuebit/${relative.slice(0, -'index.html'.length)}`;
  return `/queuebit/${relative}`;
}

function fileForRoute(pathname) {
  const relative = pathname === '/queuebit'
    ? ''
    : decodeURIComponent(pathname.slice('/queuebit/'.length));
  if (relative.length === 0 || relative.endsWith('/')) {
    return path.resolve(distRoot, relative, 'index.html');
  }
  const file = path.extname(relative).length === 0 ? `${relative}.html` : relative;
  return path.resolve(distRoot, file);
}

function displayPath(file) {
  return path.relative(process.cwd(), file).split(path.sep).join('/');
}
