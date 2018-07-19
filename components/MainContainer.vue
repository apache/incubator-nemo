<template>
  <div>
    <el-tabs @tab-click="handleTabClick">
      <el-tab-pane>
        <template slot="label">
          Timeline <i class="el-icon-time"></i>
        </template>
        <metric-timeline
         ref="metricTimeline"
        :metric="metricDataSet"
        :groups="groupDataSet">
        </metric-timeline>
      </el-tab-pane>
      <el-tab-pane>
        <template slot="label">
          DAG
        </template>
        <dag :dag="dag"></dag>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import Vue from 'vue';
import MetricTimeline from '~/components/MetricTimeline';
import DAG from '~/components/DAG';
import { DataSet } from 'vue2vis';
import { STATE } from '~/assets/constants';

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

      // websocket object
      ws: undefined,

      // element-ui specific
      collapseActiveNames: ['timeline', 'dag'],
    };
  },

  beforeMount() {
    this.prepareWebSocket();

    METRIC_LIST.forEach(metricType => {
      this.groupDataSet.add({
        id: metricType,
        content: metricType
      });
    });
  },

  methods: {
    handleTabClick({ index }) {
      if (index === '0') {
        this.$eventBus.$emit('redraw-timeline');
      }
      else if (index === '1') {
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
          order: METRIC_LIST.indexOf(metricType)
        });
      }

      let newItem = {
        id: data.id,
        group: metricType,
      };

      if (data.dag && !this.dag) {
        this.dag = data.dag;
        this.$eventBus.$emit('dag', this.dag);
      }

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          if (event.prevState === STATE.INCOMPLETE) {
            // Stage does not have READY, so it cannot be represented as
            // a range of timeline. So the only needed field is `start`.
            this.$eventBus.$emit('stageEvent', {
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

      this.ws = new WebSocket('ws://127.0.0.1:10101/api/websocket');

      this.ws.onopen = (event) => {
        // clear metric
        this.metricDataSet.clear();
        this.groupDataSet.clear();
        console.log('Connected!');
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
