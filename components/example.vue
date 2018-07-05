<template>
  <div>
    <p> {{ JSON.stringify($store.state.metric) }}</p>
  </div>
</template>

<script>
export default {
  data: () => {
    return {
      text: 'default text',
    }
  },
  methods: {

  },
  computed: {
    metricString() {
      return JSON.stringify(this.$store.state.metric, null, 2);
    }
  }
}
</script>
