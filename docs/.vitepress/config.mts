import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "SQLRec",
  description: "SQLRec docs",
  base: '/sqlrec/',
  ignoreDeadLinks: 'localhostLinks',
  locales: {
    root: {
      label: '简体中文',
      lang: 'zh-CN',
      themeConfig: {
        nav: [
          { text: '主页', link: '/' },
          { text: '文档', link: '/docs/getting-started/introduction' }
        ],
        outline: [2, 6],
        sidebar: [
          {
            text: '快速开始',
            collapsed: true,
            items: [
              { text: 'SQLRec 介绍', link: '/docs/getting-started/introduction' },
              { text: 'Docker 快速开始', link: '/docs/getting-started/docker' }
            ]
          },
          {
            text: '使用指南',
            collapsed: true,
            items: [
              { text: '编写推荐流程', link: '/docs/guides/recommendation-flow' },
              { text: '发布和调用 API', link: '/docs/guides/api' },
              { text: '接入数据源', link: '/docs/guides/data-sources' },
              { text: '模型训练与在线推理', link: '/docs/guides/model-lifecycle' },
              { text: '超时、降级与异常恢复', link: '/docs/guides/exception-recovery' }
            ]
          },
          {
            text: '参考手册',
            collapsed: true,
            items: [
              { text: 'SQL 语法', link: '/docs/reference/sql' },
              { text: '架构设计', link: '/docs/reference/architecture' },
              {
                text: 'UDF',
                collapsed: true,
                items: [
                  { text: 'UDF 概览', link: '/docs/reference/udf/' },
                  { text: '标量函数', link: '/docs/reference/udf/scalar-functions' },
                  { text: '表函数', link: '/docs/reference/udf/table-functions' }
                ]
              },
              {
                text: 'Connector',
                collapsed: true,
                items: [
                  { text: '内置 Connector', link: '/docs/reference/connectors/builtin-connectors' }
                ]
              },
              {
                text: '模型',
                collapsed: true,
                items: [
                  { text: '内置模型', link: '/docs/reference/models/builtin-models' }
                ]
              }
            ]
          },
          {
            text: '部署与运维',
            collapsed: true,
            items: [
              { text: '服务部署', link: '/docs/operations/deployment' },
              { text: '性能测试', link: '/docs/operations/benchmark' }
            ]
          },
          {
            text: '扩展开发',
            collapsed: true,
            items: [
              { text: '自定义 UDF', link: '/docs/development/custom-udf' },
              { text: '自定义 Connector', link: '/docs/development/custom-connector' },
              { text: '自定义模型', link: '/docs/development/custom-model' }
            ]
          }
        ],
        socialLinks: [
          { icon: 'github', link: 'https://github.com/sqlrec/sqlrec' }
        ]
      }
    },
    en: {
      label: 'English',
      lang: 'en-US',
      link: '/en/',
      themeConfig: {
        nav: [
          { text: 'Home', link: '/en/' },
          { text: 'Docs', link: '/en/docs/getting-started/introduction' }
        ],
        outline: [2, 6],
        sidebar: [
          {
            text: 'Quick Start',
            collapsed: true,
            items: [
              { text: 'Introduction', link: '/en/docs/getting-started/introduction' },
              { text: 'Docker Quick Start', link: '/en/docs/getting-started/docker' }
            ]
          },
          {
            text: 'Guides',
            collapsed: true,
            items: [
              { text: 'Write a Recommendation Flow', link: '/en/docs/guides/recommendation-flow' },
              { text: 'Publish and Call an API', link: '/en/docs/guides/api' },
              { text: 'Connect Data Sources', link: '/en/docs/guides/data-sources' },
              { text: 'Train and Serve Models', link: '/en/docs/guides/model-lifecycle' },
              { text: 'Timeouts and Recovery', link: '/en/docs/guides/exception-recovery' }
            ]
          },
          {
            text: 'Reference',
            collapsed: true,
            items: [
              { text: 'SQL Syntax', link: '/en/docs/reference/sql' },
              { text: 'Architecture', link: '/en/docs/reference/architecture' },
              {
                text: 'UDF',
                collapsed: true,
                items: [
                  { text: 'UDF Overview', link: '/en/docs/reference/udf/' },
                  { text: 'Scalar Functions', link: '/en/docs/reference/udf/scalar-functions' },
                  { text: 'Table Functions', link: '/en/docs/reference/udf/table-functions' }
                ]
              },
              {
                text: 'Connector',
                collapsed: true,
                items: [
                  { text: 'Built-in Connectors', link: '/en/docs/reference/connectors/builtin-connectors' }
                ]
              },
              {
                text: 'Models',
                collapsed: true,
                items: [
                  { text: 'Built-in Models', link: '/en/docs/reference/models/builtin-models' }
                ]
              }
            ]
          },
          {
            text: 'Deployment and Operations',
            collapsed: true,
            items: [
              { text: 'Service Deployment', link: '/en/docs/operations/deployment' },
              { text: 'Benchmark', link: '/en/docs/operations/benchmark' }
            ]
          },
          {
            text: 'Extension Development',
            collapsed: true,
            items: [
              { text: 'Custom UDF', link: '/en/docs/development/custom-udf' },
              { text: 'Custom Connector', link: '/en/docs/development/custom-connector' },
              { text: 'Custom Model', link: '/en/docs/development/custom-model' }
            ]
          }
        ],
        socialLinks: [
          { icon: 'github', link: 'https://github.com/sqlrec/sqlrec' }
        ]
      }
    }
  }
})
