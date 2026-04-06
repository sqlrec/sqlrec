import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "SqlRec",
  description: "SqlRec docs",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
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
})
