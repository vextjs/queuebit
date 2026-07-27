import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts', 'src/cli.ts', 'src/vext/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  external: ['vextjs'],
  sourcemap: true,
  clean: true,
  splitting: false,
  target: 'node20',
  outDir: 'dist'
});
