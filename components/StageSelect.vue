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

export default {
  props: ['selectedJobId', 'metricLookupMap'],

  data() {
    return {
      selectData: [],
      newMetricDataSet: new DataSet([]),
      metricDataSet: new DataSet([]),
    }
  },

  mounted() {
    this.$eventBus.$on('set-timeline-items', metricDataSet => {
      this.metricDataSet = metricDataSet;
      this.filterAndSend(metricDataSet);
    });

    this.$eventBus.$on('add-timeline-item', ({ jobId, item }) => {
      if (jobId !== this.selectedJobId) {
        return;
      }

      if (this.filterItem(item)) {
        this.newMetricDataSet.add(item);
      }
    });

    this.$eventBus.$on('update-timeline-item', ({ jobId, item }) => {
      if (jobId !== this.selectedJobId) {
        return;
      }

      if (this.filterItem(item)) {
        this.newMetricDataSet.update(item);
      }
    });

    this.$eventBus.$on('clear-stage-select', () => {
      this.selectData = [];
    });
  },

  computed: {
    stageList() {
      return Object.keys(this.metricLookupMap)
        .filter(id => /^Stage-[0-9]+$/.test(id.trim()));
    },
  },

  methods: {

    handleSelectChange(selectData) {
      if (selectData.length === 0) {
        this.$eventBus.$emit('metric-deselect');
      }
      this.filterAndSend(this.metricDataSet);
    },

    filterAndSend(metricDataSet) {
      this.newMetricDataSet = this.filterDataSet(metricDataSet);
      // filter and add to newMetricDataSet
      this.$eventBus.$emit(
        'set-timeline-filtered-items', this.newMetricDataSet);
    },

    filterDataSet(metricDataSet) {
      let newMetricDataSet = new DataSet([]);
      metricDataSet.forEach(item => {
        if (this.filterItem(item)) {
          newMetricDataSet.add(item);
        }
      });
      return newMetricDataSet;
    },

    filterItem(item) {
      for (const stage of this.selectData) {
        if (item.metricId.startsWith(stage + '-')
          || item.metricId === stage) {
          return true;
        }
      }
      return false;
    },

    selectAll() {
      this.selectData = [];
      this.stageList.forEach(s => this.selectData.push(s));
      this.handleSelectChange(this.selectData);
    },

    clearAll() {
      this.selectData = [];
      this.handleSelectChange(this.selectData);
    },

  }
}
</script>
<style>
.oper-button {
  margin-bottom: 15px;
}
</style>
