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
    <h1>Nemo Stages</h1>

    <!--Scheduler pools-->
    <h2>{{ schedulerPools.length }} Fair Scheduler Pools</h2>
    <div>
      <el-table class="scheduler-pools" :data="schedulerPools" stripe>
        <el-table-column label="Pool Name">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Minimum Share"></el-table-column>
        <el-table-column label="Pool Weight"></el-table-column>
        <el-table-column label="Active Stages"></el-table-column>
        <el-table-column label="Running Tasks"></el-table-column>
        <el-table-column label="SchedulingMode"></el-table-column>
      </el-table>
    </div>

    <!--Stages List-->
    <!--Pending Stages-->
    <h2 ref="pendingStages">Pending Stages
      <el-badge type="warning" :value="pendingStagesData.length"></el-badge></h2>
    <div>
      <!--<div v-if="pendingStagesData.length !== 0">-->
      <el-table class="pending-stages-table" :data="pendingStagesData" stripe>
        <el-table-column label="Stage id" width="80">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Input" width="60"></el-table-column>
        <el-table-column label="Output" width="70"></el-table-column>
        <el-table-column label="Shuffle Read"></el-table-column>
        <el-table-column label="Shuffle Write"></el-table-column>
      </el-table>
    </div>

    <!--Active Stages-->
    <h2 ref="activeStages">Active Stages
      <el-badge type="primary" :value="activeStagesData.length"></el-badge></h2>
    <div>
      <!--<div v-if="activeStagesData.length !== 0">-->
      <el-table class="active-stages-table" :data="activeStagesData" stripe>
        <el-table-column label="Stage id" width="80">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Input" width="60"></el-table-column>
        <el-table-column label="Output" width="70"></el-table-column>
        <el-table-column label="Shuffle Read"></el-table-column>
        <el-table-column label="Shuffle Write"></el-table-column>
      </el-table>
    </div>

    <!--Completed Stages-->
    <h2 ref="completedStages">Completed Stages
      <el-badge type="success" :value="completedStagesData.length"></el-badge></h2>
    <div>
      <!--<div v-if="completedStagesData.length !== 0">-->
      <el-table class="completed-stages-table" :data="completedStagesData" stripe>
        <el-table-column label="Stage id" width="80">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Input" width="60"></el-table-column>
        <el-table-column label="Output" width="70"></el-table-column>
        <el-table-column label="Shuffle Read"></el-table-column>
        <el-table-column label="Shuffle Write"></el-table-column>
      </el-table>
    </div>

    <!--Skipped Stages-->
    <h2 ref="skippedStages">Skipped Stages
      <el-badge type="info" :value="skippedStagesData.length"></el-badge></h2>
    <div>
      <!--<div v-if="skippedStagesData.length !== 0">-->
      <el-table class="skipped-stages-table" :data="skippedStagesData" stripe>
        <el-table-column label="Stage id" width="80">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Input" width="60"></el-table-column>
        <el-table-column label="Output" width="70"></el-table-column>
        <el-table-column label="Shuffle Read"></el-table-column>
        <el-table-column label="Shuffle Write"></el-table-column>
      </el-table>
    </div>

    <!--Failed Stages-->
    <h2 ref="failedStages">Failed Stages
      <el-badge type="danger" :value="failedStagesData.length"></el-badge></h2>
    <div>
      <!--<div v-if="failedStagesData.length !== 0">-->
      <el-table class="failed-stages-table" :data="failedStagesData" stripe>
        <el-table-column label="Stage id" width="80">
          <template slot-scope="scope">
            {{ scope.row }}
          </template>
        </el-table-column>
        <el-table-column label="Description" width="180"></el-table-column>
        <el-table-column label="Submitted" width="180"></el-table-column>
        <el-table-column label="Duration" width="90"></el-table-column>
        <el-table-column label="Tasks: Succeeded/Total" width="200"></el-table-column>
        <el-table-column label="Input" width="60"></el-table-column>
        <el-table-column label="Output" width="70"></el-table-column>
        <el-table-column label="Shuffle Read"></el-table-column>
        <el-table-column label="Shuffle Write"></el-table-column>
        <el-table-column label="Failure Reason" width="200"></el-table-column>
      </el-table>
    </div>

  </el-card>
</template>

<script>
import Vue from 'vue';

export default {
  computed: {
    schedulerPools() {
      return [];
    },
    // Stages by its status
    pendingStagesData() {
      return [];
    },
    activeStagesData() {
      return [];
    },
    completedStagesData() {
      return [];
    },
    skippedStagesData() {
      return [];
    },
    failedStagesData() {
      return [];
    },
  },
}
</script>
