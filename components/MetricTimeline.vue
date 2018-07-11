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

  data() {
    return {
      options: {
        // TODO: this should be modified to adjust somewhere adequate
        start: new Date(1530765471863),
        end: new Date(1530765471863),
      }
    };
  },

  computed: {

  },

  methods: {

  }

}
</script>
