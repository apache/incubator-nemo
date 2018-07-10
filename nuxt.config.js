module.exports = {
  head: {
    title: 'Nemo Web Visualizer',
    meta: [
      { charset: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { hid: 'description', name: 'description', content: 'Nemo web visualizer' }
    ],
    link: [
      { rel: 'icon', type: 'image/x-icon', href: '/nemo.ico' }
    ]
  },

  plugins: [
    '~/plugins/element-ui',
    '~/plugins/vue2vis',
  ],

  css: [
    'element-ui/lib/theme-chalk/index.css',
    'element-ui/lib/theme-chalk/reset.css',
    'vue2vis/dist/vue2vis.css'
  ],

  loading: { color: '#3B8070' },

  build: {
    vendor: ['element-ui'],
    extend (config, { isDev, isClient }) {
      if (isDev && isClient) {
        config.module.rules.push({
          enforce: 'pre',
          test: /\.(js|vue)$/,
          loader: 'eslint-loader',
          exclude: /(node_modules)/
        })
      }
    }
  }

}
