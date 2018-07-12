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

export default {
  props: ['metric', 'groups'],

  beforeMount() {
    this.$eventBus.$on('fit-timeline', () => {
      this.fitTimeline();
    });

    this.$eventBus.$on('move-timeline', ( time ) => {
      this.moveTimeline(time);
    });
  },

  data() {
    return {
      options: {
        // TODO: this should be modified to adjust somewhere adequate
        start: new Date(1530765471863),
        end: new Date(1530765471863),
      }
    };
  },

  methods: {
    fitTimeline() {
      try {
        this.$refs.timeline.fit();
      } catch (e) {
        console.warn('Error when fitting the timeline');
      }
    },

    moveTimeline(time) {
      try {
        this.$refs.timeline.moveTo(time, false);
      } catch (e) {
        console.warn('Error when moving the timeline');
      }
    },
  }

}
</script>
