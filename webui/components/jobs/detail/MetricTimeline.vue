<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
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

  //COMPUTED
  computed: {
    timeline() {
      return this.$refs.timeline;
    },
  },

  //METHODS
  methods: {
    /**
     * Redraw timeline. If timeline layout is collapsed or
     * twingled, this method should be called.
     * This method also fit the timeline synchronously.
     */
    redrawTimeline() {
      this.timeline.redraw();
      this.timeline.fit();
    },

    /**
     * Fit timeline to make all available elements visible.
     * It may be throw error if element is not ready or
     * timeline itself it not ready, but it's ignorable.
     */
    fitTimeline() {
      try {
        this.timeline.fit();
      } catch (e) {
        console.warn('Error when fitting the timeline');
      }
    },

    /**
     * Move timeline to specific timestamp.
     * It may be throw error if element is not ready or
     * timeline itself it not ready, but it's ignorable.
     * @param time timestamp or Date to move.
     */
    moveTimeline(time) {
      try {
        this.timeline.moveTo(time, false);
      } catch (e) {
        console.warn('Error when moving the timeline');
      }
    },
  },

  beforeMount() {
    // listen to redraw-timeline event.
    this.$eventBus.$on('redraw-timeline', async () => {
      // wait for rendering components
      await this.$nextTick();
      this.redrawTimeline();
    });

    // listen to fit-timeline event.
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

    // listen to move-timeline event.
    this.$eventBus.$on('move-timeline', ({ time, jobId }) => {
      if (jobId !== this.selectedJobId) {
        return;
      }
      this.moveTimeline(time);
    });

    // listen to set-timeline-filtered-items event.
    // sets timeline DataSet, which was filtered by stage filters.
    // this event only emitted by StageSelect component.
    this.$eventBus.$on('set-timeline-filtered-items', metricDataSet => {
      this.timeline.setItems(metricDataSet);
      this.fitTimeline();
    });
  },

  mounted() {
    // metric selection event handler
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
}
</script>
