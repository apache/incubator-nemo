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
    <!--el-header>
      <el-menu :default-active="activeIndex" class="el-menu-demo" mode="horizontal" @select="handleSelect">
        <el-menu-item index="0" disabled><a href="/">LOGO</a></el-menu-item>
        <el-menu-item index="1">Jobs</el-menu-item>
        <el-menu-item index="2">Stages</el-menu-item>
        <el-menu-item index="3">Storage</el-menu-item>
        <el-menu-item index="4">Environment</el-menu-item>
        <el-menu-item index="5">Executors</el-menu-item>
        <el-menu-item index="6" disabled style="float: right;">Nemo Web UI</el-menu-item>
      </el-menu>
    </el-header-->

    <el-main>
      <!--Jobs-->
      <div v-show="activeIndex === '1'">
        <jobs-view/>
      </div>
      <!--Stages-->
      <div v-if="activeIndex === '2'">
        <stages-view/>
      </div>
      <!--Storage-->
      <div v-else-if="activeIndex === '3'">
        <storage-view/>
      </div>
      <!--Environment-->
      <div v-else-if="activeIndex === '4'">
        <environment-view/>
      </div>
      <!--Executors-->
      <div v-else-if="activeIndex === '5'">
        <executors-view/>
      </div>
    </el-main>
  </el-container>
</template>

<script>
import Vue from 'vue';
import JobsView from './jobs/JobsView';
import StagesView from './stages/StagesView';
import StorageView from './storage/StorageView';
import EnvironmentView from './environment/EnvironmentView';
import ExecutorsView from './executors/ExecutorsView';
import { DataSet } from 'vue2vis';
import { STATE } from '../assets/constants';

// variable to store the return value of setTimeout()
let reconnectionTimer;
// reconnection interval
const RECONNECT_INTERVAL = 3000;

export default {
  components: {
    'jobs-view': JobsView,
    'stages-view': StagesView,
    'storage-view': StorageView,
    'environment-view': EnvironmentView,
    'executors-view': ExecutorsView,
  },

  data() {
    return {
      activeIndex: '1',
    };
  },

  //METHODS
  methods: {
    // Handle select of the different tabs.
    handleSelect(key, keyPath) {
      this.activeIndex = key;
    },
  },
}
</script>

<style>
.status-header {
  margin-bottom: 15px;
}

.header-container {
  height: 80px;
  max-width: 1200px;
  margin-left: auto;
  margin-right: auto;
  margin-top: 30px;
}

.header-title {
  display: flex;
  display: -webkit-flex;
  font-size: 32px;
}

.detail-card {
  margin-bottom: 15px;
}

.upper-card-col {
  padding-top: 8px;
  padding-bottom: 8px;
}
</style>
