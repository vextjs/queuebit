import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { extname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as ts from 'typescript';
import { QueuebitError } from '../errors';

type CommonJsModuleWithCompile = NodeJS.Module & {
  _compile(code: string, filename: string): void;
};

type NodeModuleInternals = {
  _extensions: Record<string, (module: NodeJS.Module, filename: string) => void>;
};

export interface LoadedQueuebitModule {
  path: string;
  loader: 'typescript-cjs' | 'esm' | 'cjs';
  namespace: Record<string, unknown>;
}

const nodeModule = createRequire(resolve(process.cwd(), 'queuebit-cli-loader.cjs'))('node:module') as NodeModuleInternals;
const tsExtensions = new Set(['.ts', '.tsx', '.mts', '.cts']);

export async function loadQueuebitModule(file: string): Promise<LoadedQueuebitModule> {
  const resolved = resolve(process.cwd(), file);
  const extension = extname(resolved);
  try {
    if (tsExtensions.has(extension)) {
      return {
        path: resolved,
        loader: 'typescript-cjs',
        namespace: loadTypeScriptModule(resolved)
      };
    }
    if (extension === '.cjs') {
      const requireFromFile = createRequire(pathToFileURL(resolved));
      return { path: resolved, loader: 'cjs', namespace: requireFromFile(resolved) as Record<string, unknown> };
    }
    if (extension === '.mjs' || extension === '.js') {
      const namespace = await import(pathToFileURL(resolved).href) as Record<string, unknown>;
      return { path: resolved, loader: 'esm', namespace };
    }
    throw new QueuebitError({
      code: 'QB_CLI_LOADER_FAILED',
      message: `Unsupported Queuebit module extension: ${extension || '<none>'}.`,
      details: { file: resolved, loader: 'extension', nodeVersion: process.version }
    });
  } catch (cause) {
    if (cause instanceof QueuebitError) throw cause;
    throw new QueuebitError({
      code: 'QB_CLI_LOADER_FAILED',
      message: `Queuebit CLI failed to load ${file}.`,
      details: {
        file: resolved,
        loader: tsExtensions.has(extension) ? 'typescript-cjs' : extension === '.cjs' ? 'cjs' : 'esm',
        nodeVersion: process.version,
        cause: cause instanceof Error ? { name: cause.name, message: cause.message } : String(cause)
      }
    });
  }
}

function loadTypeScriptModule(file: string): Record<string, unknown> {
  const previous = new Map<string, ((module: NodeJS.Module, filename: string) => void) | undefined>();
  for (const extension of tsExtensions) {
    previous.set(extension, nodeModule._extensions[extension]);
    nodeModule._extensions[extension] = compileTypeScriptModule;
  }
  try {
    const requireFromFile = createRequire(pathToFileURL(file));
    return requireFromFile(file) as Record<string, unknown>;
  } finally {
    for (const [extension, handler] of previous) {
      if (handler === undefined) delete nodeModule._extensions[extension];
      else nodeModule._extensions[extension] = handler;
    }
  }
}

function compileTypeScriptModule(module: NodeJS.Module, filename: string): void {
  const source = readFileSync(filename, 'utf8');
  const transpiled = ts.transpileModule(source, {
    fileName: filename,
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      moduleResolution: ts.ModuleResolutionKind.Node10,
      esModuleInterop: true,
      isolatedModules: true,
      resolveJsonModule: true,
      sourceMap: false,
      inlineSourceMap: true
    },
    reportDiagnostics: true
  });
  const errors = (transpiled.diagnostics ?? []).filter(diagnostic => diagnostic.category === ts.DiagnosticCategory.Error);
  if (errors.length > 0) {
    throw new QueuebitError({
      code: 'QB_CLI_LOADER_FAILED',
      message: 'Queuebit TypeScript loader failed to transpile a module.',
      details: {
        file: filename,
        diagnostics: errors.map(formatDiagnostic)
      }
    });
  }
  (module as CommonJsModuleWithCompile)._compile(transpiled.outputText, filename);
}

function formatDiagnostic(diagnostic: ts.Diagnostic): Record<string, unknown> {
  const details: Record<string, unknown> = {
    code: diagnostic.code,
    message: ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n')
  };
  if (diagnostic.file !== undefined && diagnostic.start !== undefined) {
    const position = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start);
    details.file = diagnostic.file.fileName;
    details.line = position.line + 1;
    details.column = position.character + 1;
  }
  return details;
}
