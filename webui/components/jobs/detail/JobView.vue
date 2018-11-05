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
  <!--(disabled for debugging)-->
  <!--<el-card v-if="selectedJobId">-->
  <el-card>
    <h1>Details for Job {{ jobFrom ? jobFrom : 'NULL' }}</h1>

    <p>
      <b>Status: </b><span>TODO</span><br>
      <span><b>Completed Stages: </b><span>TODO</span><br></span>
      <span><b>Active Stages: </b><span>TODO</span><br></span>
      <span><b>Pending Stages: </b><span>TODO</span><br></span>
      <span><b>Skipped Stages: </b><span>TODO</span><br></span>
    </p>

    <el-collapse @change="handleCollapse">
      <!--Event Timeline-->
      <el-collapse-item title="Event Timeline" name="1">
        <el-card header="Timeline" class="detail-card">
          <metric-timeline
            ref="metricTimeline"
            :selectedJobId="selectedJobId"
            :groups="groupDataSet"/>
        </el-card>
        <el-row :gutter="10">
          <el-col height="100%" :span="12" :xs="24">
            <el-card class="detail-card" header="Select stage">
              <stage-select
                :selectedJobId="selectedJobId"
                :metricLookupMap="metricLookupMap"/>
            </el-card>
          </el-col>
          <el-col :span="12" :xs="24">
            <el-card class="detail-card" header="Detail">
              <detail-table
                v-if="tabIndex === '1'"
                :tableData="tableData"/>
            </el-card>
          </el-col>
        </el-row>
      </el-collapse-item>
      <!--DAG Visualization-->
      <el-collapse-item title="DAG Visualization" name="2">
        <el-row id="affix-target" :gutter="10">
          <el-col :span="16" :xs="24">
            <el-card header="DAG">
              <dag :selectedJobId="selectedJobId" :tabIndex="tabIndex"/>
            </el-card>
          </el-col>
          <el-col :span="8" :xs="24">
            <no-ssr>
              <affix relative-element-selector="#affix-target">
                <el-card header="Detail">
                  <detail-table
                    v-if="tabIndex === '2'"
                    :tableData="tableData"/>
                </el-card>
              </affix>
            </no-ssr>
          </el-col>
        </el-row>
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
  import MetricTimeline from './MetricTimeline'
  import DetailTable from './DetailTable';
  import StageSelect from './StageSelect';
  import DAG from './DAG'

  // list of metric, order of elements matters.
  export const METRIC_LIST = [
    'StageMetric',
    'TaskMetric',
  ];

  const LISTENING_EVENT_LIST = [
    'job-id-select',
    'job-id-deselect',
    'build-table-data',
    'metric-select',
    'metric-deselect',
  ];

  export default {
    components: {
      'metric-timeline': MetricTimeline,
      'stage-select': StageSelect,
      'detail-table': DetailTable,
      'dag': DAG,
    },

    data() {
      return {
        // timeline dataset
        groupDataSet: new DataSet([]),

        // selected metric id
        selectedMetricId: '',
        // selected job id
        selectedJobId: '',
        // endpoint or file name of job
        jobFrom: '',

        metricLookupMap: {}, // metricId -> data

        // element-ui specific
        collapseActiveNames: ['timeline', 'dag'],
        tableData: [],
        tabIndex: '0',
      }
    },

    methods: {
      handleCollapse(collapseElements) {
        if (collapseElements.includes("1")) {
          this.$eventBus.$emit('redraw-timeline');
        }
        if (collapseElements.includes("2")) {
          this.$eventBus.$emit('rerender-dag');
        }
      },

      /**
       * Set up event handlers for this component.
       */
      setUpEventHandlers() {
        // event handler for detecting change of job id
        this.$eventBus.$on('job-id-select', data => {
          this.$eventBus.$emit('set-timeline-items', data.metricDataSet);
          this.$eventBus.$emit('clear-stage-select');
          this.selectedJobId = data.jobId;
          this.jobFrom = data.jobFrom;
          this.metricLookupMap = data.metricLookupMap;
          this.selectedMetricId = '';
        });

        this.$eventBus.$on('job-id-deselect', () => {
          this.$eventBus.$emit('set-timeline-items', new DataSet([]));
          this.$eventBus.$emit('clear-stage-select');
          this.selectedJobId = '';
          this.jobFrom = '';
          this.metricLookupMap = {};
          this.selectedMetricId = '';
        });

        this.$eventBus.$on('build-table-data', ({ metricId, jobId }) => {
          if (this.selectedJobId === jobId &&
            this.selectedMetricId === metricId) {
            this.buildTableData(metricId);
          }
        });

        // event handler for individual metric selection
        this.$eventBus.$on('metric-select', metricId => {
          this.selectedMetricId = metricId;
          this.buildTableData(metricId);
          this.$eventBus.$emit('metric-select-done');
        });

        // event handler for individual metric deselection
        this.$eventBus.$on('metric-deselect', async () => {
          this.tableData = [];
          this.selectedMetricId = '';
          await this.$nextTick();
          this.$eventBus.$emit('metric-deselect-done');
        });
      },

      /**
       * Build table data which will be used in TaskStatistics component.
       * @param metricId id of metric. Used to lookup metricLookupMap.
       */
      buildTableData(metricId) {
        this.tableData = [];
        const metric = this._removeUnusedProperties(this.metricLookupMap[metricId]);
        Object.keys(metric).forEach(key => {
          if (typeof metric[key] === 'object') {
            if (key === 'executionProperties') {
              let executionPropertyArray = [];
              Object.keys(metric[key]).forEach(ep => {
                executionPropertyArray.push({
                  key: ep,
                  value: metric[key][ep],
                });
              });
              this.tableData.push({
                key: key,
                value: '',
                extra: executionPropertyArray,
              });
            }
          } else {
            let value = metric[key] === -1 ? 'N/A' : metric[key];
            if (value !== 'N/A' && key.toLowerCase().endsWith('bytes')) {
              value = this._bytesToHumanReadable(value);
            }
            this.tableData.push({
              key: key,
              value: value,
            });
          }
        });
      },

      _bytesToHumanReadable(bytes) {
        var i = bytes === 0 ? 0 :
          Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, i)).toFixed(2) * 1
          + ' ' + ['B', 'KB', 'MB', 'GB', 'TB'][i];
      },

      _removeUnusedProperties(metric) {
        let newMetric = Object.assign({}, metric);
        delete newMetric.group;
        delete newMetric.content;
        return newMetric;
      },
    },

    // HOOKS
    beforeMount() {
      // predefine group sets
      METRIC_LIST.forEach(metricType => {
        this.groupDataSet.add({
          id: metricType,
          content: metricType
        });
      });

      this.setUpEventHandlers();
    },

    beforeDestroy() {
      LISTENING_EVENT_LIST.forEach(e => {
        this.$eventBus.$off(e);
      });
    },
  }
</script>
