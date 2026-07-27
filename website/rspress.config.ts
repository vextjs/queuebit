import { defineConfig } from '@rspress/core';
import mermaid from 'rspress-plugin-mermaid';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const currentDir = path.dirname(fileURLToPath(import.meta.url));

const englishSidebar = [
  {
    text: 'Quick Start',
    items: [
      { text: 'Quick start', link: '/quick-start' },
      { text: 'Learning path', link: '/concepts' },
      { text: 'Run one background job', link: '/job-recipes' }
    ]
  },
  {
    text: 'On-Demand Capabilities',
    items: [
      { text: 'Process many database records', link: '/batch-runs' },
      { text: 'Prevent duplicate side effects', link: '/idempotency-patterns' },
      { text: 'Distributed workers', link: '/distributed-workers' },
      { text: 'vext integration', link: '/vext-integration' }
    ]
  },
  {
    text: 'Production',
    items: [
      { text: 'Configure Redis and Workers', link: '/configuration-recipes' },
      { text: 'Deploy Queuebit in production', link: '/production-deployment' },
      { text: 'Check problems after launch', link: '/operations' },
      { text: 'Recover from failures', link: '/failure-runbooks' },
      { text: 'When Redis is down', link: '/distributed-semantics' }
    ]
  },
  {
    text: 'Reference',
    items: [
      { text: 'Can my environment use Queuebit?', link: '/compatibility' },
      { text: 'API quick lookup', link: '/target-api' },
      { text: 'CLI reference', link: '/cli-reference' },
      { text: 'Configuration field dictionary', link: '/cli-and-config' },
      { text: 'States and errors', link: '/failure-modes' }
    ]
  }
];

const chineseSidebar = [
  {
    text: '快速开始',
    items: [
      { text: '快速开始', link: '/zh/quick-start' },
      { text: '学习路径', link: '/zh/concepts' },
      { text: '执行一个后台任务', link: '/zh/job-recipes' }
    ]
  },
  {
    text: '按需能力',
    items: [
      { text: '批量处理数据库记录', link: '/zh/batch-runs' },
      { text: '防止重复副作用', link: '/zh/idempotency-patterns' },
      { text: '多个 Worker 怎么一起跑', link: '/zh/distributed-workers' },
      { text: 'vext 接入', link: '/zh/vext-integration' }
    ]
  },
  {
    text: '生产运维',
    items: [
      { text: '配置 Redis 和 Worker', link: '/zh/configuration-recipes' },
      { text: '生产上线怎么部署', link: '/zh/production-deployment' },
      { text: '上线后怎么查问题', link: '/zh/operations' },
      { text: '故障恢复', link: '/zh/failure-runbooks' },
      { text: 'Redis 断了怎么办', link: '/zh/distributed-semantics' }
    ]
  },
  {
    text: '参考',
    items: [
      { text: '我的环境能不能用', link: '/zh/compatibility' },
      { text: 'API 快查', link: '/zh/target-api' },
      { text: 'CLI 参考', link: '/zh/cli-reference' },
      { text: '配置字段字典', link: '/zh/cli-and-config' },
      { text: '状态和错误怎么读', link: '/zh/failure-modes' }
    ]
  }
];

const englishNav = [
  { text: 'Quick Start', link: '/quick-start' },
  { text: 'Capabilities', link: '/batch-runs' },
  { text: 'Production', link: '/production-deployment' },
  { text: 'Reference', link: '/reference' }
];

const chineseNav = [
  { text: '快速开始', link: '/zh/quick-start' },
  { text: '按需能力', link: '/zh/batch-runs' },
  { text: '生产运维', link: '/zh/production-deployment' },
  { text: '参考', link: '/zh/reference' }
];

export default defineConfig({
  root: path.join(currentDir, '..', 'docs', 'v01'),
  base: '/queuebit/',
  lang: 'en',
  title: 'queuebit',
  logoText: 'queuebit',
  icon: '/favicon.svg',
  globalStyles: path.join(currentDir, 'styles', 'queuebit.css'),
  globalUIComponents: [path.join(currentDir, 'components', 'A11yLabels.tsx')],
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
        footer: {
          message: 'Released under the Apache-2.0 License.'
        },
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
        footer: {
          message: '基于 Apache-2.0 许可证发布。'
        },
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
        content: 'https://github.com/devcodex-labs/queuebit'
      }
    ],
    footer: {
      message: 'Released under the Apache-2.0 License.'
    }
  }
});
