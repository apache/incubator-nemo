<template>
  <el-row type="flex" justify="space-between">
    <el-col :span="mainColSpan">
      <el-tabs @tab-click="handleTabClick">
        <el-tab-pane>
          <template slot="label">
            Timeline <i class="el-icon-time"/>
          </template>
          <metric-timeline
             ref="metricTimeline"
            :metric="metricDataSet"
            :metricLookupMap="metricLookupMap"
            :groups="groupDataSet"/>
        </el-tab-pane>
        <el-tab-pane>
          <template slot="label">
            DAG
          </template>
          <dag
            :metricDataSet="metricDataSet"
            :metricLookupMap="metricLookupMap"/>
        </el-tab-pane>
      </el-tabs>
    </el-col>
    <el-col :span="subColSpan">
      <template v-if="tableData.length > 0">
        <el-table
          empty-text="No data"
          :row-class-name="_rowClassName"
          :data="tableData">
          <el-table-column type="expand">
            <template slot-scope="props">
              <ul>
                <li v-for="ep in props.row.extra" :key="ep.key">
                  {{ ep.key }}: {{ ep.value }}
                </li>
              </ul>
            </template>
          </el-table-column>
          <el-table-column label="Key" prop="key"/>
          <el-table-column label="Value" prop="value"/>
        </el-table>
      </template>
    </el-col>
  </el-row>
</template>

<script>
import Vue from 'vue';
import MetricTimeline from '~/components/MetricTimeline';
import DAG from '~/components/DAG';
import { DataSet } from 'vue2vis';
import { STATE } from '~/assets/constants';

const WEBSOCKET_ENDPOINT = 'ws://127.0.0.1:10101/api/websocket';

// list of metric, order of elements matters.
const METRIC_LIST = [
  'JobMetric',
  'StageMetric',
  'TaskMetric',
];

// timeline fitting option
const FIT_OPTIONS = {
  duration: 500,
}

// variable to store the return value of setTimeout()
let reconnectionTimer;
// reconnection interval
const RECONNECT_INTERVAL = 3000;

export default {
  components: {
    'metric-timeline': MetricTimeline,
    'dag': DAG,
  },

  data() {
    return {
      // timeline dataset
      metricDataSet: new DataSet([]),
      groupDataSet: new DataSet([]),

      // dag data
      dag: undefined,
      metricLookupMap: {}, // metricId -> data

      // websocket object
      ws: undefined,

      // element-ui specific
      collapseActiveNames: ['timeline', 'dag'],
      tableData: [],
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
      return 23 - this.mainColSpan;
    }
  },

  beforeMount() {
    this.tryReconnect();

    // predefine group sets
    METRIC_LIST.forEach(metricType => {
      this.groupDataSet.add({
        id: metricType,
        content: metricType
      });
    });

    this.$eventBus.$on('metric-selected', metricId => {
      this.tableData = [];
      const metric = this.metricLookupMap[metricId];
      Object.keys(metric).forEach(key => {
        if (typeof metric[key] === 'object') {
          if (key === 'executionProperties') {
            let executionPropertyArray = [];
            Object.keys(metric[key]).forEach(ep => {
              executionPropertyArray.push({
                key: ep,
                value: metric[key][ep]
              });
            });
            this.tableData.push({
              key: key,
              value: '',
              extra: executionPropertyArray,
            });
          }
        } else {
          this.tableData.push({
            key: key,
            value: metric[key],
          });
        }
      });
      this.$eventBus.$emit('metric-selected-done');
    });

    this.$eventBus.$on('metric-deselect', () => {
      this.tableData = [];
      this.$eventBus.$emit('resize-canvas');
    });

  },

  methods: {
    _rowClassName(rowObject) {
      if (rowObject.row.extra) {
        return '';
      }
      return 'no-expand';
    },

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

    buildMetricLookupMapWithDAG() {
      this.dag.vertices.forEach(stage => {
        this.metricLookupMap[stage.id] = this._flatten(stage);
        stage.properties.irDag.vertices.forEach(vertex => {
          this.metricLookupMap[vertex.id] = this._flatten(vertex);
        });
        stage.properties.irDag.edges.forEach(edge => {
          const edgeId = edge.properties.runtimeEdgeId;
          this.metricLookupMap[edgeId] = this._flatten(edge);
        });
      });
      this.dag.edges.forEach(edge => {
        const edgeId = edge.properties.runtimeEdgeId;
        this.metricLookupMap[edgeId] = this._flatten(edge);
      });
    },

    addMetricToMetricLookupMap(metric) {
      if (metric.group === 'JobMetric') {
        this.metricLookupMap[metric.id] = this._removeUnusedProperties(metric);
      } else if (metric.group === 'TaskMetric') {
        this.metricLookupMap[metric.id] = this._removeUnusedProperties(metric);
      }
    },

    handleTabClick({ index }) {
      if (index === '0') {
        this.$eventBus.$emit('redraw-timeline');
      } else if (index === '1') {
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

    prepareWebSocket() {
      if (!process.browser) {
        return;
      }

      if (this.ws && this.ws.readyState !== WebSocket.CLOSED) {
        this.closeWebSocket();
      }

      this.ws = new WebSocket(WEBSOCKET_ENDPOINT);

      this.ws.onopen = (event) => {
        // clear metric
        this.metricDataSet.clear();
        this.groupDataSet.clear();
      };

      this.ws.onmessage = (event) => {
        let parsedData;
        try {
          parsedData = JSON.parse(event.data);
        } catch (e) {
          console.warn('Non-JSON data received');
          return;
        }

        this.processMetric(parsedData);
      };

      this.ws.onclose = () => {
        this.tryReconnect();
      };

      this.ws.onerror = () => {
        this.tryReconnect();
      };

      window.onbeforeunload = () => {
        this.closeWebSocket();
      };
    },

    closeWebSocket() {
      if (!this.ws) {
        return;
      }
      this.ws.close();
      this.ws = undefined;
    },

    tryReconnect() {
      this.closeWebSocket();
      if (reconnectionTimer) {
        clearTimeout(reconnectionTimer);
      }
      reconnectionTimer = setTimeout(() => {
        this.prepareWebSocket();
      }, RECONNECT_INTERVAL);
    },
  }
}
</script>
<style>
.no-expand .el-icon {
  display: none;
}

.no-expand .el-table__expand-icon {
  pointer-events: none;
}
</style>
