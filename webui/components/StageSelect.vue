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
  <div>
    <el-button
      @click="selectAll"
      class="oper-button"
      type="primary"
      size="small"
      plain>
      Select all</el-button>
    <el-button
      @click="clearAll"
      class="oper-button"
      type="danger"
      size="small"
      plain>
      Clear</el-button>
    <el-select
      @change="handleSelectChange"
      v-model="selectData"
      multiple
      filterable
      style="width: 100%">
      <el-option
        v-for="stage in stageList"
        :label="stage"
        :value="stage"
        :key="stage"/>
    </el-select>
  </div>
</template>

<script>
import { DataSet } from 'vue2vis';

const LISTENING_EVENT_LIST = [
  'set-timeline-items',
  'add-timeline-item',
  'update-timeline-item',
  'clear-stage-select',
];

export default {
  props: ['selectedJobId', 'metricLookupMap'],

  data() {
    return {
      selectData: [],
      newMetricDataSet: new DataSet([]),
      metricDataSet: new DataSet([]),
    }
  },

  //COMPUTED
  computed: {
    /**
     * Temporary computed property which filter StageMetric metric id
     * by matching regex pattern.
     */
    stageList() {
      return Object.keys(this.metricLookupMap)
        .filter(id => /^Stage[0-9]+$/.test(id.trim()));
    },
  },

  //METHODS
  methods: {
    /**
     * Handler that handles change in stage filter selection.
     * If there is no selected stage, this method will emit
     * metric deselection event.
     * @param selectData selected stage id array. will be provided by
     *        element-ui el-select component.
     */
    handleSelectChange(selectData) {
      if (selectData && selectData.length === 0) {
        this.$eventBus.$emit('metric-deselect');
      }
      this.filterAndSend(this.metricDataSet);
    },

    /**
     * Filter metric DataSet by stage id array and send it to
     * MetricTimeline component to render timeline.
     * @param metricDataSet DataSet object for timeline.
     */
    filterAndSend(metricDataSet) {
      this.newMetricDataSet = this.filterDataSet(metricDataSet);
      // filter and add to newMetricDataSet
      this.$eventBus.$emit(
        'set-timeline-filtered-items', this.newMetricDataSet);
    },

    /**
     * Filter DataSet by stage id array.
     * @param metricDataSet DataSet object for timeline.
     */
    filterDataSet(metricDataSet) {
      let newMetricDataSet = new DataSet([]);
      metricDataSet.forEach(item => {
        if (this.filterItem(item)) {
          newMetricDataSet.add(item);
        }
      });
      return newMetricDataSet;
    },

    /**
     * Determines whether item is filtered by current stage selecion.
     * @param item item to filter.
     * @returns true if it should be contained to DataSet, false if not.
     */
    filterItem(item) {
      for (const stage of this.selectData) {
        if (item.metricId.startsWith(stage + '-')
          || item.metricId === stage) {
          return true;
        }
      }
      return false;
    },

    /**
     * Select all stage id.
     */
    selectAll() {
      this.selectData = [];
      this.stageList.forEach(s => this.selectData.push(s));
      this.handleSelectChange(this.selectData);
    },

    /**
     * Clear stage filter array.
     */
    clearAll() {
      this.selectData = [];
      this.handleSelectChange(this.selectData);
    },
  },

  //HOOKS
  mounted() {
    // this event is emitted by JobView, which processes incoming metric.
    this.$eventBus.$on('set-timeline-items', metricDataSet => {
      this.metricDataSet = metricDataSet;
      this.filterAndSend(metricDataSet);
    });

    // this event is emitted by JobView, which processes incoming metric.
    this.$eventBus.$on('add-timeline-item', ({ jobId, item }) => {
      if (jobId !== this.selectedJobId) {
        return;
      }

      if (this.filterItem(item)) {
        this.newMetricDataSet.add(item);
      }
    });

    // this event is emitted by JobView, which processes incoming metric.
    this.$eventBus.$on('update-timeline-item', ({ jobId, item }) => {
      if (jobId !== this.selectedJobId) {
        return;
      }

      if (this.filterItem(item)) {
        this.newMetricDataSet.update(item);
      }
    });

    // should be emitted when metric was deselected or job was cleared.
    this.$eventBus.$on('clear-stage-select', () => {
      this.selectData = [];
    });
  },

  beforeDestroy() {
    LISTENING_EVENT_LIST.forEach(e => {
      this.$eventBus.$off(e);
    });
  },
}
</script>

<style>
.oper-button {
  margin-bottom: 15px;
}
</style>
