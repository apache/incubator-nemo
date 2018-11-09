<!--
Copyright (C) 2018 Seoul National University
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
<template>
  <el-card>
    <!--Title-->
    <h1>Executors</h1>

    <!--Summary-->
    <h2>Summary</h2>
    <div>
      <el-table class="executors-summary-table" :data="executorsSummaryData" stripe>
        <el-table-column label="Blocks"></el-table-column>
      </el-table>
    </div>

    <!--Executors-->
    <h2>Executors</h2>
    <div>
      <el-table class="executors-table" :data="executorsData" stripe>
        <el-table-column label="ID"></el-table-column>
        <el-table-column label="Address"></el-table-column>
        <el-table-column label="Status"></el-table-column>
      </el-table>
    </div>
  </el-card>
</template>

<script>
import Vue from 'vue';

export default {
  computed: {
    executorsSummaryData() {
      return [];
    },
    executorsData() {
      return [];
    },
  },
}
</script>
