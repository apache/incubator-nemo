<template>
  <div class="main-container">
      <job-metric
       ref="jobMetricComponent"
      :metric="taskMetricDataSet"
      :groups="groupDataSet">
      </job-metric>
  </div>
</template>

<script>
import Vue from 'vue';
import JobMetric from '~/components/JobMetric';
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
    JobMetric,
  },

  data() {
    return {
      jobMetricDataSet: new DataSet([]),
      stageMetricDataSet: new DataSet([]),
      taskMetricDataSet: new DataSet([]),
      groupDataSet: new DataSet([]),
      ws: undefined,
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
            content: metricType
          });
        }
        await this.processIndividualMetric(metric);
      } else {
        // this means the first big metric chunk
        // await ctx.dispatch('processInitialMetric', metric);
      }
    },

    async processIndividualMetric({ metricType, data }) {
      let target;
      switch (metricType) {
        case 'JobMetric':
          target = this.jobMetricDataSet;
          break;
        case 'StageMetric':
          target = this.stageMetricDataSet;
          break;
        case 'TaskMetric':
          target = this.taskMetricDataSet;
          break;
        default:
          return;
      }

      let newItem = { id: data.id, group: metricType };

      data.stateTransitionEvents
        .filter(event => event.prevState != null)
        .forEach(event => {
          if (event.prevState == STATE.INCOMPLETE) {
            // Stage does not have READY, so it cannot be represented as
            // a range of timeline. So the only needed field is `start`.
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id;
          } else if (event.prevState == STATE.READY) {
            newItem.start = new Date(event.timestamp);
            newItem.content = data.id;
          } else if (event.newState == STATE.COMPLETE) {
            newItem.end = new Date(event.timestamp);
            newItem.content = data.id;
          }
        });
      let prevItem = target.get(newItem.id);
      if (!prevItem) {
        target.add(newItem);
        if (target.length == 1) {
          try {
            await this.moveTimeline(newItem.start);
          } catch (e) {
            console.warn('Error when moving timeline');
          }
        } else {
          await this.fitTimeline();
        }
      } else {
        target.update(newItem);
        if (!(prevItem.start == newItem.start && prevItem.end == newItem.end)) {
          await this.fitTimeline();
        }
      }
    },

    async fitTimeline() {
      this.$refs.jobMetricComponent.$refs.timeline.fit(FIT_OPTIONS);
    },

    moveTimeline(time) {
      return new Promise((resolve, reject) => {
        try {
          this.$refs.jobMetricComponent
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

      this.ws = new WebSocket('ws://localhost:10101/api/websocket');

      this.ws.onopen = (event) => {
        // clear metric
        this.jobMetricDataSet.clear();
        this.stageMetricDataSet.clear();
        this.taskMetricDataSet.clear();
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
        setTimeout(() => {
          this.prepareWebSocket();
        }, RECONNECT_INTERVAL);
      };

      this.ws.onerror = () => {
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
