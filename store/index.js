import Vue from 'vue';
import Vuex from 'vuex';
import { DataSet } from 'vue2vis';

const STATE = {
  READY: 'READY',
  EXECUTING: 'EXECUTING',
  INCOMPLETE: 'INCOMPLETE',
  COMPLETE: 'COMPLETE',
};

const createStore = () => {
  return new Vuex.Store({
    state: {
      ws: undefined,
      metric: {},
      jobMetricDataSet: new DataSet([]),
      stageMetricDataSet: new DataSet([]),
      taskMetricDataSet: new DataSet([]),
      groupDataSet: new DataSet([]),
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
      setInitialMetricType({ metric, groupDataSet }, { metricType }) {
        if (!(metricType in metric)) {
          Vue.set(metric, metricType, {});
        }
        if (groupDataSet == null) {
          groupDataSet = new DataSet();
        }
        groupDataSet.add({
          id: metricType,
          content: metricType
        });
      },
      setIndividualMetric(state, { metricType, data }) {
        Vue.set(state.metric[metricType], data.id, {
          id: data.id,
          data: data
        });

        let target;
        switch (metricType) {
          case 'JobMetric':
            if (state.jobMetricDataSet == null) {
              state.jobMetricDataSet = new DataSet();
            }
            target = state.jobMetricDataSet;
            break;
          case 'StageMetric':
            if (state.stageMetricDataSet == null) {
              state.stageMetricDataSet = new DataSet();
            }
            target = state.stageMetricDataSet;
            break;
          case 'TaskMetric':
            if (state.taskMetricDataSet == null) {
              state.taskMetricDataSet = new DataSet();
            }
            target = state.taskMetricDataSet;
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
              newItem.content = data.id + ' COMPLETE';
            } else if (event.prevState == STATE.READY) {
              newItem.start = new Date(event.timestamp);
              newItem.content = data.id + ' START';
            } else if (event.newState == STATE.COMPLETE) {
              newItem.end = new Date(event.timestamp);
              newItem.content = data.id + ' COMPLETE';
            }
          });
        target.update(newItem);
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
