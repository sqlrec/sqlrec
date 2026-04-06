import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "SQLRec",
  description: "SQLRec docs",
  locales: {
    root: {
      label: '简体中文',
      lang: 'zh-CN',
      themeConfig: {
        nav: [
          { text: '主页', link: '/' },
          { text: '文档', link: '/docs/intro' }
        ],
        outline: [2, 6],
        sidebar: [
          { text: '介绍', link: '/docs/intro' },
          { text: '部署', link: '/docs/deployment' },
          { text: '快速开始', link: '/docs/quick_start' },
          { text: '性能测试', link: '/docs/benchmark' },
          { text: '编程模型', link: '/docs/program_model' },
          { text: 'SQL语法', link: '/docs/sql_reference' },
          { text: '模型', link: '/docs/models' },
          { text: '内置UDF', link: '/docs/udf' },
          {
            text: '教程',
            collapsed: true,
            items: [
              { text: '召回', link: '/docs/tutorial/recall' }
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
          { text: 'Docs', link: '/en/docs/intro' }
        ],
        outline: [2, 6],
        sidebar: [
          { text: 'Introduction', link: '/en/docs/intro' },
          { text: 'Deployment', link: '/en/docs/deployment' },
          { text: 'Quick Start', link: '/en/docs/quick_start' },
          { text: 'Benchmark', link: '/en/docs/benchmark' },
          { text: 'Programming Model', link: '/en/docs/program_model' },
          { text: 'SQL Reference', link: '/en/docs/sql_reference' },
          { text: 'Models', link: '/en/docs/models' },
          { text: 'Built-in UDF', link: '/en/docs/udf' },
          {
            text: 'Tutorials',
            collapsed: true,
            items: [
              { text: 'Recall', link: '/en/docs/tutorial/recall' }
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
