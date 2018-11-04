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
  <!--<el-card v-if="selectedJobId"> (disabled for debugging)-->
  <el-card>
    <h1>Details for Job TODO</h1>

    <p>
      <b>Status: </b><span>TODO</span><br>
      <span><b>Completed Stages: </b><span>TODO</span><br></span>
      <span><b>Active Stages: </b><span>TODO</span><br></span>
      <span><b>Pending Stages: </b><span>TODO</span><br></span>
      <span><b>Skipped Stages: </b><span>TODO</span><br></span>
    </p>

    <el-collapse>
      <!--Event Timeline-->
      <el-collapse-item title="Event Timeline" name="1">
        TODO
      </el-collapse-item>
      <!--DAG Visualization-->
      <el-collapse-item title="DAG Visualization" name="2">
        TODO
      </el-collapse-item>
    </el-collapse>

    <!--Stages List-->
    <!--Completed Stages-->
    <h2>Completed Stages (TODO)</h2>
    <el-table>
      <el-table-column label="Stage id" width="80"></el-table-column>
      <el-table-column label="Description" width="180"></el-table-column>
      <el-table-column label="Submitted" width="180"></el-table-column>
      <el-table-column label="Duration" width="90"></el-table-column>
      <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
      <el-table-column label="Input" width="60"></el-table-column>
      <el-table-column label="Output" width="70"></el-table-column>
      <el-table-column label="Shuffle Read"></el-table-column>
      <el-table-column label="Shuffle Write"></el-table-column>
    </el-table>

    <!--Active Stages-->
    <h2>Active Stages (TODO)</h2>
    <el-table>
      <el-table-column label="Stage id" width="80"></el-table-column>
      <el-table-column label="Description" width="180"></el-table-column>
      <el-table-column label="Submitted" width="180"></el-table-column>
      <el-table-column label="Duration" width="90"></el-table-column>
      <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
      <el-table-column label="Input" width="60"></el-table-column>
      <el-table-column label="Output" width="70"></el-table-column>
      <el-table-column label="Shuffle Read"></el-table-column>
      <el-table-column label="Shuffle Write"></el-table-column>
    </el-table>

    <!--Pending Stages-->
    <h2>Pending Stages (TODO)</h2>
    <el-table>
      <el-table-column label="Stage id" width="80"></el-table-column>
      <el-table-column label="Description" width="180"></el-table-column>
      <el-table-column label="Submitted" width="180"></el-table-column>
      <el-table-column label="Duration" width="90"></el-table-column>
      <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
      <el-table-column label="Input" width="60"></el-table-column>
      <el-table-column label="Output" width="70"></el-table-column>
      <el-table-column label="Shuffle Read"></el-table-column>
      <el-table-column label="Shuffle Write"></el-table-column>
    </el-table>

    <!--Skipped Stages-->
    <h2>Skipped Stages (TODO)</h2>
    <el-table>
      <el-table-column label="Stage id" width="80"></el-table-column>
      <el-table-column label="Description" width="180"></el-table-column>
      <el-table-column label="Submitted" width="180"></el-table-column>
      <el-table-column label="Duration" width="90"></el-table-column>
      <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
      <el-table-column label="Input" width="60"></el-table-column>
      <el-table-column label="Output" width="70"></el-table-column>
      <el-table-column label="Shuffle Read"></el-table-column>
      <el-table-column label="Shuffle Write"></el-table-column>
    </el-table>
  </el-card>
</template>

<script>
  import Vue from 'vue';
  import { DataSet } from 'vue2vis';

  export default {
    props: ['selectedJobId', 'metricLookupMap'],

  }
</script>
