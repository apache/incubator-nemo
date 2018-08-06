import test from 'ava';
import Vue from 'vue';
import JobView from '../components/JobView';

test.before('initialize', async t => {
  // load all plugins
  require('../plugins/event-bus');
});

test.serial('selectJobId', async t => {
  const vm = new Vue(JobView).$mount();
  let counter = 0;
  let dagEmitted = null;

  vm.$eventBus.$on('job-id-select', async (data) => {
    counter++;
  });

  vm.$eventBus.$on('dag', async (data) => {
    dagEmitted = data.dag;
  });

  vm._newJob('foo');
  vm._newJob('bar');
  vm.selectedJobId = 'foo';
  vm.jobs.foo.dag = 'lorem';

  await vm.selectJobId('bar');
  t.is(counter, 1);
  t.is(vm.selectedJobId, 'bar');
  t.is(dagEmitted, null);

  await vm.selectJobId('bar');
  t.is(counter, 1);
  t.is(vm.selectedJobId, 'bar');
  t.is(dagEmitted, null);

  await vm.selectJobId('foo');
  t.is(counter, 2);
  t.is(vm.selectedJobId, 'foo');
  t.is(dagEmitted, 'lorem');

  vm.$eventBus.$off('job-id-select');
  vm.$eventBus.$off('dag');
});

test('deletejobId', async t => {
  const vm = new Vue(JobView).$mount();
  let deselectCounter = 0;
  let clearDagCounter = 0;

  vm.$eventBus.$on('job-id-deselect', async (data) => {
    deselectCounter++;
  });

  vm.$eventBus.$on('clear-dag', async (data) => {
    clearDagCounter++;
  });

  vm._newJob('foo');
  vm._newJob('bar');
  vm.selectedJobId = 'foo';

  vm.deleteJobId('foo');
  await vm.$nextTick();

  // should be deleted
  t.falsy('foo' in vm.jobs);
  t.is(deselectCounter, 1);
  t.is(clearDagCounter, 1);
  t.is(vm.selectedJobId, '');

  vm.selectedJobId = 'foo';
  vm.deleteJobId('bar');
  await vm.$nextTick();

  // if selectedJobId and target id is not same, do not emit clear events
  // also, make sure that job id is not changed
  t.falsy('bar' in vm.jobs);
  t.is(deselectCounter, 1);
  t.is(clearDagCounter, 1);
  t.is(vm.selectedJobId, 'foo');

  vm.$eventBus.$off('job-id-deselect');
  vm.$eventBus.$off('clear-dag');
});

test('processMetric', async t => {
  const vm = new Vue(JobView).$mount();
  let toBeCalledCounter = 0;
  vm.processIndividualMetric = async () => {
    toBeCalledCounter++;
  };

  // individual metric
  await vm.processMetric({metricType: 'foo'}, 'foo');
  t.is(toBeCalledCounter, 1);

  // big chunk of initial metric
  const fakeMetric = {
    FooMetric: {
      FooID: {
        metricType: 'FooMetric',
        data: {}
      },
      BarID: {
        metricType: 'FooMetric',
        data: {}
      },
      BazID: {
        metricType: 'FooMetric',
        data: {}
      }
    },
    BarMetric: {
      LoremID: {
        metricType: 'BarMetric',
        data: {}
      },
      IpsumID: {
        metricType: 'BarMetric',
        data: {}
      }
    }
  };

  await vm.processMetric(fakeMetric, 'foo');
  t.is(toBeCalledCounter, 6);
});

test.serial('processIndividualMetric', async t => {
  const vm = new Vue(JobView).$mount();
  let stateChangeEventData = null;
  let dagData = null;

  vm.$eventBus.$on('state-change-event', data => {
    if (stateChangeEventData) {
      t.fail();
    }
    stateChangeEventData = data;
  });

  vm.$eventBus.$on('dag', data => {
    if (dagData) {
      t.fail();
    }
    dagData = data;
  });

  vm._newJob('foo');
  const job = vm.jobs.foo;
  t.truthy(job);

  const fakeMetric = {
    metricType: 'JobMetric',
    data: {
      id: 'bar',
      dag: 'bar-dag',
      stateTransitionEvents: [
        {
          prevState: 'READY',
          newState: 'EXECUTING',
          timestamp: +new Date()
        }
      ],
    }
  };

  vm.buildMetricLookupMapWithDAG = (jobId) => { t.is(jobId, 'foo'); };
  await vm.processIndividualMetric(fakeMetric, 'foo');
  await vm.$nextTick();

  t.is(stateChangeEventData.jobId, 'foo');
  t.is(stateChangeEventData.metricId, 'bar');
  t.is(stateChangeEventData.metricType, 'JobMetric');
  t.is(stateChangeEventData.prevState, 'READY');
  t.is(stateChangeEventData.newState, 'EXECUTING');

  t.is(dagData.dag, 'bar-dag');
  t.is(dagData.jobId, 'foo');
  t.false(dagData.init);

  vm.$eventBus.$off('state-change-event');
  vm.$eventBus.$off('dag');
});
