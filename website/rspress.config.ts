import { defineConfig } from '@rspress/core';
import mermaid from 'rspress-plugin-mermaid';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const currentDir = path.dirname(fileURLToPath(import.meta.url));

const englishSidebar = [
  {
    text: 'Start Using Queuebit',
    items: [
      { text: 'Overview', link: '/' },
      { text: 'Quick start', link: '/quick-start' },
      { text: 'vext integration', link: '/vext-integration' },
      { text: 'CLI and configuration', link: '/cli-and-config' },
      { text: 'Operations and troubleshooting', link: '/operations' }
    ]
  },
  {
    text: 'Understand the Model',
    items: [
      { text: 'Compatibility boundary', link: '/compatibility' },
      { text: 'Queuebit concepts', link: '/concepts' },
      { text: 'Redis-only and recovery', link: '/distributed-semantics' },
      { text: 'Failure modes', link: '/failure-modes' }
    ]
  },
  {
    text: 'Reference',
    items: [
      { text: 'Reference index', link: '/reference' },
      { text: 'API reference', link: '/target-api' },
      { text: 'Worker and scheduler lifecycle', link: '/worker-lifecycle' }
    ]
  }
];

const chineseSidebar = [
  {
    text: '开始使用 Queuebit',
    items: [
      { text: '概览', link: '/zh/' },
      { text: '快速开始', link: '/zh/quick-start' },
      { text: 'vext 接入', link: '/zh/vext-integration' },
      { text: 'CLI 与配置', link: '/zh/cli-and-config' },
      { text: '运维与排查', link: '/zh/operations' }
    ]
  },
  {
    text: '理解模型',
    items: [
      { text: '运行环境与兼容边界', link: '/zh/compatibility' },
      { text: 'Queuebit 概念', link: '/zh/concepts' },
      { text: 'Redis-only 与恢复', link: '/zh/distributed-semantics' },
      { text: '故障模式', link: '/zh/failure-modes' }
    ]
  },
  {
    text: '参考',
    items: [
      { text: '参考入口', link: '/zh/reference' },
      { text: 'API 参考', link: '/zh/target-api' },
      { text: 'Worker 与 Scheduler 生命周期', link: '/zh/worker-lifecycle' }
    ]
  }
];

const englishNav = [
  { text: 'Guide', link: '/quick-start' },
  { text: 'vext', link: '/vext-integration' },
  { text: 'Config', link: '/cli-and-config' },
  { text: 'Operations', link: '/operations' },
  { text: 'Compatibility', link: '/compatibility' },
  { text: 'Reference', link: '/reference' }
];

const chineseNav = [
  { text: '指南', link: '/zh/quick-start' },
  { text: 'vext', link: '/zh/vext-integration' },
  { text: '配置', link: '/zh/cli-and-config' },
  { text: '运维', link: '/zh/operations' },
  { text: '兼容边界', link: '/zh/compatibility' },
  { text: '参考', link: '/zh/reference' }
];

export default defineConfig({
  root: path.join(currentDir, '..', 'docs'),
  base: '/queuebit/',
  lang: 'en',
  title: 'queuebit',
  logoText: 'queuebit',
  globalStyles: path.join(currentDir, 'styles', 'queuebit.css'),
  description: 'Redis-only distributed job queue user manual for queuebit.',
  outDir: 'dist',
  locales: [
    {
      lang: 'en',
      label: 'English',
      title: 'queuebit',
      description: 'Redis-only distributed job queue user manual.'
    },
    {
      lang: 'zh',
      label: '简体中文',
      title: 'queuebit',
      description: 'Redis-only 分布式任务队列用户手册。'
    }
  ],
  markdown: {
    link: {
      checkDeadLinks: false
    }
  },
  plugins: [
    mermaid({
      mermaidConfig: {
        theme: 'neutral',
        securityLevel: 'strict'
      }
    })
  ],
  search: {
    codeBlocks: true
  },
  languageParity: {
    enabled: true
  },
  themeConfig: {
    nav: englishNav,
    locales: [
      {
        lang: 'en',
        label: 'English',
        title: 'queuebit',
        description: 'Redis-only distributed job queue user manual.',
        nav: englishNav,
        sidebar: {
          '/': englishSidebar
        }
      },
      {
        lang: 'zh',
        label: '简体中文',
        title: 'queuebit',
        description: 'Redis-only 分布式任务队列用户手册。',
        nav: chineseNav,
        sidebar: {
          '/zh/': chineseSidebar
        }
      }
    ],
    sidebar: {
      '/': englishSidebar,
      '/zh/': chineseSidebar
    },
    socialLinks: [
      {
        icon: 'github',
        mode: 'link',
        content: 'https://github.com/vextjs/queuebit'
      }
    ],
    footer: {
      message: 'Released under the Apache-2.0 License.'
    }
  }
});
