<template>
  <div>
    <el-collapse v-model="collapseActiveNames">
      <el-collapse-item name="timeline">
        <template slot="title">
          Timeline <i class="el-icon-time"></i>
        </template>
        <metric-timeline
         ref="metricTimeline"
        :metric="metricDataSet"
        :groups="groupDataSet">
        </metric-timeline>
      </el-collapse-item>
    </el-collapse>
  </div>
</template>

<script>
import Vue from 'vue';
import MetricTimeline from '~/components/MetricTimeline';
import { DataSet } from 'vue2vis';

const STATE = {
  READY: 'READY',
  EXECUTING: 'EXECUTING',
  INCOMPLETE: 'INCOMPLETE',
  COMPLETE: 'COMPLETE',
};

const METRIC_LIST = [
  'JobMetric',
  'StageMetric',
  'TaskMetric',
];

const FIT_OPTIONS = {
  duration: 500,
}

const RECONNECT_INTERVAL = 3000;

export default {
  components: {
    MetricTimeline,
  },

  data() {
    return {
      // timeline dataset
      metricDataSet: new DataSet([]),
      groupDataSet: new DataSet([]),

      // websocket object
      ws: undefined,

      // element-ui specific
      collapseActiveNames: ['timeline'],
    };
  },

  mounted() {
    this.prepareWebSocket();

    METRIC_LIST.forEach(metricType => {
      this.groupDataSet.add({
        id: metricType,
        content: metricType
      });
    });
  },

  methods: {
    async processMetric(metric) {
      // specific event broadcast
      if ('metricType' in metric) {
        const metricType = metric.metricType;
        if (!this.groupDataSet.get(metricType)) {
          this.groupDataSet.add({
            id: metricType,
            content: metricType,
            order: METRIC_LIST.indexOf(metricType)
          });
        }
        await this.processIndividualMetric(metric);
      } else {
        // this means the first big metric chunk
        // await ctx.dispatch('processInitialMetric', metric);
      }
    },

    async processIndividualMetric({ metricType, data }) {
      let newItem = {
        id: data.id,
        group: metricType,
      };

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          if (event.prevState == STATE.INCOMPLETE) {
            // Stage does not have READY, so it cannot be represented as
            // a range of timeline. So the only needed field is `start`.
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id + ' COMPLETE';
          } else if (event.prevState == STATE.READY) {
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id;
          } else if (event.newState == STATE.COMPLETE) {
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
        this.metricDataSet.add(newItem);
        if (this.metricDataSet.length == 1) {
          try {
            await this.moveTimeline(newItem.start);
          } catch (e) {
            console.warn('Error when moving timeline');
          }
        } else {
          await this.fitTimeline();
        }
      } else {
        this.metricDataSet.update(newItem);
        if (!(prevItem.start == newItem.start && prevItem.end == newItem.end)) {
          await this.fitTimeline();
        }
      }
    },

    async fitTimeline() {
      try {
        this.$refs.metricTimeline.$refs.timeline.fit(FIT_OPTIONS);
      } catch (e) {
        console.warn('Error when fitting timeline');
      }
    },

    moveTimeline(time) {
      return new Promise((resolve, reject) => {
        try {
          this.$refs.metricTimeline
            .$refs.timeline.moveTo(time, false, () => resolve());
        } catch (e) {
          reject();
        }
      });
    },

    prepareWebSocket() {
      if (!process.browser) {
        return;
      }

      if (this.ws && this.ws.readyState != WebSocket.CLOSED) {
        ws.close();
      }

      this.ws = new WebSocket('ws://localhost:10101/api/websocket');

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
        this.ws.close();
        setTimeout(() => {
          this.prepareWebSocket();
        }, RECONNECT_INTERVAL);
      };

      this.ws.onerror = () => {
        this.ws.close();
        setTimeout(() => {
          this.prepareWebSocket();
        }, RECONNECT_INTERVAL);
      };

      window.onbeforeunload = () => {
        this.ws.close();
      };
    }
  }
}
</script>
