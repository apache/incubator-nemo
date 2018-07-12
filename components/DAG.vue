<template>
  <div>
    <p>{{ temp }}</p>
  </div>
</template>

<script>
import dagreD3 from 'dagre-d3';

export default {
  beforeMount() {
    this.$eventBus.$on('wow', event => {
      this.getEvent(event);
    })
  },

  data() {
    return {
      temp: 'wow',
    };
  },

  methods: {
    getEvent(event) {
      this.temp = event;
    },
  }
}
</script>
