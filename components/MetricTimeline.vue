<template>
  <div>
    <timeline
      ref="timeline"
      :groups="groups"
      :options="options">
    </timeline>
  </div>
</template>

<script>
import { DataSet } from 'vue2vis';
import Vue from 'vue';

export const FIT_THROTTLE_INTERVAL = 500;

const LISTENING_EVENT_LIST = [
  'redraw-timeline',
  'fit-timeline',
  'move-timeline',
  'set-timeline-filtered-items',
];

export default {
  props: ['selectedJobId', 'groups', 'metricLookupMap'],

  beforeMount() {
    this.$eventBus.$on('redraw-timeline', async () => {
      await this.$nextTick();
      this.redrawTimeline();
    });

    this.$eventBus.$on('fit-timeline', jobId => {
      if (jobId !== this.selectedJobId) {
        return;
      }

      if (this.fitThrottleTimer) {
        return;
      }

      this.fitThrottleTimer = setTimeout(() => {
        this.fitTimeline();
        this.fitThrottleTimer = null;
      }, FIT_THROTTLE_INTERVAL);
    });

    this.$eventBus.$on('move-timeline', ({ time, jobId }) => {
      if (jobId !== this.selectedJobId) {
        return;
      }
      this.moveTimeline(time);
    });

    this.$eventBus.$on('set-timeline-filtered-items', metricDataSet => {
      this.timeline.setItems(metricDataSet);
      this.fitTimeline();
    });
  },

  mounted() {
    this.timeline.on('select', ({ items }) => {
      if (items.length === 0) {
        this.$eventBus.$emit('metric-deselect');
      } else {
        this.$eventBus.$emit('metric-select', items[0]);
      }
    });
  },

  beforeDestroy() {
    LISTENING_EVENT_LIST.forEach(e => {
      this.$eventBus.$off(e);
    });
  },

  data() {
    return {
      options: {
        onInitialDrawComplete: this.fitTimeline,
        start: Date.now(),
        end: Date.now(),
      },

      fitThrottleTimer: null,
    };
  },

  computed: {
    timeline() {
      return this.$refs.timeline;
    },
  },

  methods: {
    redrawTimeline() {
      this.timeline.redraw();
      this.timeline.fit();
    },

    fitTimeline() {
      try {
        this.timeline.fit();
      } catch (e) {
        console.warn('Error when fitting the timeline');
      }
    },

    moveTimeline(time) {
      try {
        this.timeline.moveTo(time, false);
      } catch (e) {
        console.warn('Error when moving the timeline');
      }
    },
  }

}
</script>
