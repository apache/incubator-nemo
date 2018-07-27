<template>
  <div>
    <timeline
      ref="timeline"
      :items="metric"
      :groups="groups"
      :options="options">
    </timeline>
  </div>
</template>

<script>
import { DataSet } from 'vue2vis';

const FIT_THROTTLE_INTERVAL = 500;
const REDRAW_DELAY = 300;

export default {
  props: ['metric', 'groups', 'metricLookupMap'],

  beforeMount() {
    this.$eventBus.$on('redraw-timeline', () => {
      setTimeout(this.redrawTimeline, REDRAW_DELAY);
    });

    this.$eventBus.$on('fit-timeline', () => {
      if (this.fitThrottleTimer) {
        return;
      }

      this.fitThrottleTimer = setTimeout(() => {
        this.fitTimeline();
        this.fitThrottleTimer = null;
      }, FIT_THROTTLE_INTERVAL);
    });

    this.$eventBus.$on('move-timeline', ( time ) => {
      this.moveTimeline(time);
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

  data() {
    return {
      options: {
        start: Date.now(),
        end: Date.now(),
      },

      fitThrottleTimer: undefined,
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
