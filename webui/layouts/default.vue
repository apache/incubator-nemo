<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<template>
  <el-container>
    <el-header class="header-container">
      <h2 class="header-title">
        {{ title }}
      </h2>
    </el-header>
    <el-container>
      <el-main>
        <el-row type="flex" justify="center">
          <el-col :span="20">
            <div class="main-container">
              <nuxt/>
            </div>
          </el-col>
        </el-row>
      </el-main>
    </el-container>
  </el-container>
</template>

<script>
const DEFAULT_WINDOW_WIDTH = 1920;

export default {
  data() {
    return {
      windowWidth: DEFAULT_WINDOW_WIDTH,
    }
  },

  beforeMount() {
    if (process.browser) {
      this.windowWidth = window.innerWidth;
        window.addEventListener('resize', this.updateWindowWidth);
    }
    this.updateWindowWidth();
  },

  computed: {
    title() {
      if (this.windowWidth > 768) {
        return 'Nemo Web Visualizer';
      }
      return 'Nemo Visualizer';
    },
  },

  methods: {
    updateWindowWidth() {
      if (process.browser) {
        this.windowWidth = window.innerWidth;
      } else {
        this.windowWidth = DEFAULT_WINDOW_WIDTH;
      }
    }
  },

}
</script>

<style scoped>
html {
  -ms-text-size-adjust: 100%;
  -webkit-text-size-adjust: 100%;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  box-sizing: border-box;
}

*, *:before, *:after {
  box-sizing: border-box;
  margin: 0;
}

.header-container {
  height: 80px;
  max-width: 1200px;
  margin-left: auto;
  margin-right: auto;
  margin-top: 30px;
}

.main-container {
  margin-left: auto;
  margin-right: auto;
}

.header-title {
  display: flex;
  display: -webkit-flex;
  font-size: 32px;
}
</style>
