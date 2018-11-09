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
    <h1>Environment</h1>

    <!--Runtime Info-->
    <h2>Runtime Information</h2>
    <div>
      <el-table class="runtime-info-table" :data="runtimeInfoData" stripe>
        <el-table-column label="Name"></el-table-column>
        <el-table-column label="Value"></el-table-column>
      </el-table>
    </div>

    <!--Nemo Properties-->
    <h2>Nemo Properties</h2>
    <div>
      <el-table class="nemo-properties-table" :data="nemoPropertiesData" stripe>
        <el-table-column label="Name"></el-table-column>
        <el-table-column label="Value"></el-table-column>
      </el-table>
    </div>
  </el-card>
</template>

<script>
import Vue from 'vue';

export default {
  computed: {
    runtimeInfoData() {
      return [];
    },
    nemoPeopertiesData() {
      return [];

    }
  },
}
</script>
