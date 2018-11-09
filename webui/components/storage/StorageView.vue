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
    <h1>Storage</h1>

    <!--Stages-->
    <h2>Cached Stages</h2>
    <div>
      <el-table class="cached-stages-table" :data="cachedStagesData" stripe>
        <el-table-column label="ID" width="80">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Stage Name" width="110"></el-table-column>
        <el-table-column label="Storage Level" width="180"></el-table-column>
        <el-table-column label="Cached Partitions"></el-table-column>
        <el-table-column label="Fraction Cached"></el-table-column>
        <el-table-column label="Size in Memory"></el-table-column>
        <el-table-column label="Size on Disk"></el-table-column>
      </el-table>
    </div>

  </el-card>
</template>

<script>
import Vue from 'vue';

export default {
  computed: {
    cachedStagesData() {
      return [];
    },
  },
}
</script>
