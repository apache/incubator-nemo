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
  <div class="main-container">
    <nuxt/>
  </div>

  <!--<el-container>-->
    <!--<el-header class="header-container">-->
      <!--<h2 class="header-title">-->
        <!--{{ title }}-->
      <!--</h2>-->
    <!--</el-header>-->
    <!--<el-container>-->
      <!--<el-main>-->
        <!--<el-row type="flex" justify="center">-->
          <!--<el-col :span="20">-->
            <!--<div class="main-container">-->
              <!--<nuxt/>-->
            <!--</div>-->
          <!--</el-col>-->
        <!--</el-row>-->
      <!--</el-main>-->
    <!--</el-container>-->
  <!--</el-container>-->
</template>

<script>

export default {
  data() {
    return {
    }
  },

  methods: {

  },

  beforeMount() {

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

.main-container {
  margin-left: auto;
  margin-right: auto;
}

</style>
