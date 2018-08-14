/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    '~/plugins/event-bus',
    { src: '~/plugins/vue-affix', ssr: false },
  ],

  css: [
    'element-ui/lib/theme-chalk/index.css',
    'element-ui/lib/theme-chalk/reset.css',
    'vue2vis/dist/vue2vis.css'
  ],

  loading: { color: '#3B8070' },

  build: {
    vendor: ['element-ui', 'vue2vis'],
    maxChunkSize: 256000,
    extend (config, { isDev, isClient }) {
      if (isDev && isClient) {
        config.module.rules.push({
          enforce: 'pre',
          test: /\.(js|vue)$/,
          loader: 'eslint-loader',
          exclude: /(node_modules)/
        })
      }
    },
  }

}
