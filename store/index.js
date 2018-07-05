import Vue from 'vue';
import Vuex from 'vuex';

const createStore = () => {
  return new Vuex.Store({
    state: {
      ws: undefined,
      metric: {},
    },

    mutations: {
      setWebSocket({ ws }, ws_) {
        ws = ws_;
      },
      clearMetric({ metric }) {
        metric = {};
      },
      setInitialMetric({ metric }, initMetric) {
        metric = initMetric;
      },
      setInitialMetricType({ metric }, { metricType }) {
        if (!(metricType in metric)) {
          Vue.set(metric, metricType, {});
        }
      },
      setIndividualMetric({ metric }, { metricType, data }) {
        Vue.set(metric[metricType], data.id, {
          id: data.id,
          data: data
        });
      }
    },

    actions: {
      async processMetric(ctx, metric) {
        // specific event broadcast
        if ('metricType' in metric) {
          ctx.commit('setInitialMetricType', metric)
          await ctx.dispatch('processIndividualMetric', metric);
        } else {
          // this means the first big metric chunk
          await ctx.dispatch('processInitialMetric', metric);
        }
      },
      async processIndividualMetric(ctx, metric) {
        ctx.commit('setIndividualMetric', metric);
      },
      async processInitialMetric(ctx, metric) {
        ctx.commit('setInitialMetric', metric);
      }
    }
  });
};

export default createStore;
