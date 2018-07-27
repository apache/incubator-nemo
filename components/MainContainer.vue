<template>
  <div>
    <!--
    <el-card class="checkbox-container">
      <el-row>
        <el-col class="upper-card-col" :span="8" :xs="24">
          <el-row type="flex" justify="center">
              <el-checkbox
                :span="12"
                @change="autoReconnectChanged"
                v-model="autoReconnect">Auto reconnect</el-checkbox>
          </el-row>
        </el-col>
        <el-col class="upper-card-col" :span="8" :xs="24">
          <el-row type="flex" justify="center">
            <div :span="12">
              Status: {{ _wsStatusText(wsStatus) }}
              <i v-if="wsStatus === 'closed'" class="el-icon-loading"/>
            </div>
          </el-row>
        </el-col>
        <el-col class="upeer-card-col" :span="8" :xs="24">
          <el-row type="flex" justify="center">
            <el-button
              :span="12"
              type="primary"
              round
              :disabled="buttonDisabled"
              @click="prepareWebSocket">Connect</el-button>
          </el-row>
        </el-col>
      </el-row>
    </el-card>
    -->
    <el-card>
      <el-tabs @tab-click="handleTabClick">
        <el-tab-pane>
          <template slot="label">
            Jobs <i class="el-icon-tickets"/>
          </template>
          <job-view></job-view>
        </el-tab-pane>
        <el-tab-pane>
          <template slot="label">
            Timeline <i class="el-icon-time"/>
          </template>
          <el-row type="flex" justify="space-between">
            <el-col :span="mainColSpan">
              <metric-timeline
                 ref="metricTimeline"
                :metric="metricDataSet"
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
              <dag
                :tabIndex="tabIndex"
                :metricDataSet="metricDataSet"/>
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

const WEBSOCKET_ENDPOINT = 'ws://127.0.0.1:10101/api/websocket';

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
      metricDataSet: new DataSet([]),
      groupDataSet: new DataSet([]),

      // selected metric id
      selectedMetricId: '',

      // dag data
      dag: undefined,
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

    this.$eventBus.$on('metric-select', metricId => {
      this.selectedMetricId = metricId;
      this.buildTableData(metricId);
      this.$eventBus.$emit('metric-select-done');
    });

    this.$eventBus.$on('metric-deselect', async () => {
      this.tableData = [];
      this.selectedMetricId = '';
      await this.$nextTick();
      this.$eventBus.$emit('metric-deselect-done');
    });

  },

  methods: {
    _flatten(metric) {
      let newMetric = {};
      Object.keys(metric).forEach(key => {
        if (key === 'properties') {
          Object.assign(newMetric, this._flatten(metric[key]));
        } else if (key !== 'irDag') {
          newMetric[key] = metric[key];
        }
      });

      return newMetric;
    },

    _removeUnusedProperties(metric) {
      let newMetric = Object.assign({}, metric);
      delete newMetric.group;
      delete newMetric.content;
      return newMetric;
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

    buildMetricLookupMapWithDAG() {
      this.dag.vertices.forEach(stage => {
        Vue.set(this.metricLookupMap, stage.id, this._flatten(stage));
        stage.properties.irDag.vertices.forEach(vertex => {
          Vue.set(this.metricLookupMap, vertex.id, this._flatten(vertex));
        });
        stage.properties.irDag.edges.forEach(edge => {
          const edgeId = edge.properties.runtimeEdgeId;
          Vue.set(this.metricLookupMap, edgeId, this._flatten(edge));
        });
      });
      this.dag.edges.forEach(edge => {
        const edgeId = edge.properties.runtimeEdgeId;
        Vue.set(this.metricLookupMap, edgeId, this._flatten(edge));
      });
    },

    addMetricToMetricLookupMap(metric) {
      if (metric.group === 'JobMetric') {
        Vue.set(this.metricLookupMap, metric.id, metric);
      } else if (metric.group === 'TaskMetric') {
        Vue.set(this.metricLookupMap, metric.id, metric);
      }
      if (this.selectedMetricId === metric.id) {
        this.buildTableData(metric.id);
      }
    },

    handleTabClick({ index }) {
      this.tabIndex = index;
      if (index === TIMELINE_TAB) {
        this.$eventBus.$emit('redraw-timeline');
      } else if (index === DAG_TAB) {
        this.$eventBus.$emit('rerender-dag');
      }
    },

    async processMetric(metric) {
      // specific event broadcast
      if ('metricType' in metric) {
        await this.processIndividualMetric(metric);
      } else {
        // the first big metric chunk
        Object.keys(metric).forEach(metricType => {
          Object.values(metric[metricType]).forEach(async data => {
            await this.processIndividualMetric({
              metricType: metricType,
              data: data,
            });
          });
        });
      }
    },

    async processIndividualMetric({ metricType, data }) {
      // build group dataset
      if (!this.groupDataSet.get(metricType)) {
        this.groupDataSet.add({
          id: metricType,
          content: metricType,
          order: METRIC_LIST.indexOf(metricType),
        });
      }

      let newItem = { group: metricType };
      // overwrite item object with received data
      Object.assign(newItem, data);

      // if data contains `dag`, it will send to DAG component
      // TODO: support multi job with job identifier
      // maybe can use self-generated UUIDv4?
      if (data.dag && !this.dag) {
        this.dag = data.dag;
        this.$eventBus.$emit('dag', this.dag);
        this.buildMetricLookupMapWithDAG();
      }

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          if (event.prevState === STATE.INCOMPLETE) {
            // Stage does not have READY, so it cannot be represented as
            // a range of timeline. So the only needed field is `start`.
            this.$eventBus.$emit('stage-event', {
              stageId: data.id,
              state: STATE.COMPLETE,
            });
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id + ' COMPLETE';
          } else if (event.prevState === STATE.READY) {
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id;
          } else if (event.newState === STATE.COMPLETE) {
            if (newItem.start) {
              newItem.end = new Date(event.timestamp);
            } else {
              newItem.start = new Date(event.timestamp);
            }
            newItem.content = data.id;
          }
        });

      let prevItem = this.metricDataSet.get(newItem.id);
      if (!prevItem) {
        try {
          this.metricDataSet.add(newItem);
          this.addMetricToMetricLookupMap(newItem);
        } catch (e) {
          console.warn('Error when adding new item');
        }
        if (this.metricDataSet.length === 1) {
          this.moveTimeline(newItem.start);
        } else {
          this.fitTimeline();
        }
      } else {
        try {
          this.metricDataSet.update(newItem);
          this.addMetricToMetricLookupMap(newItem);
        } catch (e) {
          console.warn('Error when updating item');
        }
        if (!(prevItem.start === newItem.start && prevItem.end === newItem.end)) {
          this.fitTimeline();
        }
      }
    },

    fitTimeline() {
      this.$eventBus.$emit('fit-timeline');
    },

    moveTimeline(time) {
      this.$eventBus.$emit('move-timeline', time);
    },

    /*
    prepareWebSocket() {
      if (!process.browser) {
        return;
      }

      if (this.ws && this.ws.readyState !== WebSocket.CLOSED) {
        // this.closeWebSocket();
        // is this really correct?
        return;
      }

      this.ws = new WebSocket(WEBSOCKET_ENDPOINT);

      this.ws.onopen = () => {
        // clear metric
        this.metricDataSet.clear();
        this.groupDataSet.clear();
        this.dag = null;
        this.selectedMetricId = '';
        this.wsStatus = 'opened';
      };

      this.ws.onmessage = (event) => {
        let parsedData;
        try {
          parsedData = JSON.parse(event.data);
        } catch (e) {
          console.warn('Non-JSON data received');
          return;
        }

        // pass to metric handling logic
        this.processMetric(parsedData);
      };

      this.ws.onclose = () => {
        this.wsStatus = 'closed';
        if (this.ws && this.autoReconnect) {
          this.ws = undefined;
          this.tryReconnect();
        }
      };

      window.onbeforeunload = () => {
        this.closeWebSocket();
      };
    },

    closeWebSocket() {
      if (this.ws) {
        this.ws.close();
      }
    },

    clearWebSocketReconnectTimer() {
      if (reconnectionTimer) {
        clearTimeout(reconnectionTimer);
        reconnectionTimer = null;
      }
    },

    tryReconnect() {
      if (this.wsStatus === 'opened') {
        return;
      }
      this.closeWebSocket();

      // when auto reconnect option is true
      if (this.autoReconnect) {
        this.clearWebSocketReconnectTimer();
        // timer for reconnecting
        reconnectionTimer = setTimeout(() => {
          this.tryReconnect();
        }, RECONNECT_INTERVAL);
      }
      this.prepareWebSocket();
    },

    autoReconnectChanged(v) {
      if (v && this.wsStatus === 'closed') {
        this.tryReconnect();
      } else if (!v) {
        this.clearWebSocketReconnectTimer();
      }
    },
    */
  }
}
</script>
<style>
.checkbox-container {
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
