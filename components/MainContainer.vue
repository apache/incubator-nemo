<template>
  <div>
    <el-card class="status-header">
      <el-row>
        <el-col class="upper-card-col" :span="8" :xs="24">
          <el-row type="flex" justify="center">
            <div :span="12">
              Current job: {{ selectedJobId ? selectedJobId : "Not selected" }}
            </div>
          </el-row>
        </el-col>
      </el-row>
    </el-card>
    <el-card>
      <el-tabs @tab-click="handleTabClick">
        <el-tab-pane>
          <template slot="label">
            Jobs <i class="el-icon-tickets"/>
          </template>
          <job-view/>
        </el-tab-pane>
        <el-tab-pane>
          <template slot="label">
            Timeline <i class="el-icon-time"/>
          </template>
          <el-row type="flex" justify="space-between">
            <el-col :span="mainColSpan">
              <metric-timeline
                ref="metricTimeline"
                :selectedJobId="selectedJobId"
                :groups="groupDataSet"/>
            </el-col>
            <el-col :span="subColSpan">
              <detail-table
                v-if="tabIndex === '1'"
                :tableData="tableData"/>
            </el-col>
          </el-row>
        </el-tab-pane>
        <el-tab-pane>
          <template slot="label">
            DAG
          </template>
          <el-row type="flex" justify="space-between">
            <el-col :span="mainColSpan">
              <dag :selectedJobId="selectedJobId" :tabIndex="tabIndex"/>
              </el-col>
            <el-col :span="subColSpan">
              <detail-table
                v-if="tabIndex === '2'"
                :tableData="tableData"/>
            </el-col>
          </el-row>
        </el-tab-pane>
        <el-tab-pane>
          <template slot="label">
            Task
          </template>
          <task-statistics
            :selectedJobId="selectedJobId"
            :metricLookupMap="metricLookupMap"/>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script>
import Vue from 'vue';
import JobView from '~/components/JobView';
import MetricTimeline from '~/components/MetricTimeline';
import DAG from '~/components/DAG';
import DetailTable from '~/components/DetailTable';
import TaskStatistics from '~/components/TaskStatistics';
import { DataSet } from 'vue2vis';
import { STATE } from '~/assets/constants';

// list of metric, order of elements matters.
const METRIC_LIST = [
  'JobMetric',
  'StageMetric',
  'TaskMetric',
];

const JOBS_TAB = '0';
const TIMELINE_TAB = '1';
const DAG_TAB = '2';

// variable to store the return value of setTimeout()
let reconnectionTimer;
// reconnection interval
const RECONNECT_INTERVAL = 3000;

export default {
  components: {
    'job-view': JobView,
    'metric-timeline': MetricTimeline,
    'dag': DAG,
    'detail-table': DetailTable,
    'task-statistics': TaskStatistics,
  },

  data() {
    return {
      // timeline dataset
      groupDataSet: new DataSet([]),

      // selected metric id
      selectedMetricId: '',
      // selected job id
      selectedJobId: '',

      metricLookupMap: {}, // metricId -> data

      // element-ui specific
      collapseActiveNames: ['timeline', 'dag'],
      tableData: [],
      tabIndex: '0',
    };
  },

  computed: {
    mainColSpan() {
      if (this.tableData.length === 0) {
        return 24;
      }
      return 12;
    },

    subColSpan() {
      return 24 - this.mainColSpan;
    },
  },

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

  methods: {
    setUpEventHandlers() {
      // event handler for detecting change of job id
      this.$eventBus.$on('job-id-select', data => {
        this.$eventBus.$emit('set-timeline-items', data.metricDataSet);
        this.selectedJobId = data.jobId;
        this.metricLookupMap = data.metricLookupMap;
        this.selectedMetricId = '';
      });

      this.$eventBus.$on('job-id-deselect', () => {
        this.$eventBus.$emit('set-timeline-items', new DataSet([]));
        this.selectedJobId = '';
        this.metricLookupMap = {};
        this.selectedMetricId = '';
      });

      this.$eventBus.$on('build-table-data', ({ metricId, jobId }) => {
        if (this.selectJobId === jobId &&
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
          this.tableData.push({
            key: key,
            value: value,
          });
        }
      });
    },

    handleTabClick({ index }) {
      this.tabIndex = index;
      if (index === TIMELINE_TAB) {
        this.$eventBus.$emit('redraw-timeline');
      } else if (index === DAG_TAB) {
        this.$eventBus.$emit('rerender-dag');
      }
    },

    _removeUnusedProperties(metric) {
      let newMetric = Object.assign({}, metric);
      delete newMetric.group;
      delete newMetric.content;
      return newMetric;
    },
  }
}
</script>
<style>
.status-header {
  margin-bottom: 15px;
}

.upper-card-col {
  padding-top: 8px;
  padding-bottom: 8px;
}

.no-expand .el-icon {
  display: none;
}

.no-expand .el-table__expand-icon {
  pointer-events: none;
}
</style>
