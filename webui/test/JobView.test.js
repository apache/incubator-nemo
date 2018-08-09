/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  t.is(counter, 1, 'job-id-select event should be fired');
  t.is(vm.selectedJobId, 'bar', 'selectedJobId should be changed');
  t.is(dagEmitted, null, 'dag should not be changed');

  await vm.selectJobId('bar');
  t.is(counter, 1, 'job-id-select event should not be fired');
  t.is(vm.selectedJobId, 'bar', 'selectedJobId should not be changed');
  t.is(dagEmitted, null, 'dag should not be changed');

  await vm.selectJobId('foo');
  t.is(counter, 2, 'job-id-select event should be fired');
  t.is(vm.selectedJobId, 'foo', 'selectedJobId should be changed');
  t.is(dagEmitted, 'lorem', 'dag should be changed');

  vm.$eventBus.$off('job-id-select');
  vm.$eventBus.$off('dag');
  vm.$destroy();
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
  t.falsy('foo' in vm.jobs,
    'job `foo` should be deleted from `jobs` object');
  t.is(deselectCounter, 1, 'job-id-deselect event should be fired');
  t.is(clearDagCounter, 1, 'clear-dag event should be fired');
  t.is(vm.selectedJobId, '', 'selectedJobId should be cleared');

  vm.selectedJobId = 'foo';
  vm.deleteJobId('bar');
  await vm.$nextTick();

  // if selectedJobId and target id is not same, do not emit clear events
  // also, make sure that job id is not changed
  t.falsy('bar' in vm.jobs,
    'job `bar` should be deleted from `jobs` object');
  t.is(deselectCounter, 1,
    'job-id-deselect event should not be fired when selectedJobId is differ from deleted job id');
  t.is(clearDagCounter, 1,
    'clear-dag event should not be fired when selectedJobId is differ from deleted job id');
  t.is(vm.selectedJobId, 'foo', 'job id should not be changed');

  vm.$eventBus.$off('job-id-deselect');
  vm.$eventBus.$off('clear-dag');
  vm.$destroy();
});

test('processMetric', async t => {
  const vm = new Vue(JobView).$mount();
  let toBeCalledCounter = 0;
  vm.processIndividualMetric = async () => {
    toBeCalledCounter++;
  };

  // individual metric
  await vm.processMetric({metricType: 'foo'}, 'foo');
  t.is(toBeCalledCounter, 1, 'individual metric should be processed');

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
  t.is(toBeCalledCounter, 6, 'metric chunk should be properly processed');

  vm.$destroy();
});

test.serial('processIndividualMetric', async t => {
  const vm = new Vue(JobView).$mount();
  let stateChangeEventData = null;
  let dagData = null;

  vm.$eventBus.$on('state-change-event', data => {
    if (stateChangeEventData) {
      t.fail('state-change-event event should be fired only once');
    }
    stateChangeEventData = data;
  });

  vm.$eventBus.$on('dag', data => {
    if (dagData) {
      t.fail('dag event should be fired only once');
    }
    dagData = data;
  });

  vm._newJob('foo');
  const job = vm.jobs.foo;
  t.truthy(job, 'job should be valid');

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

  vm.buildMetricLookupMapWithDAG = (jobId) => { t.is(jobId, 'foo', 'job id should be matched'); };
  await vm.processIndividualMetric(fakeMetric, 'foo');
  await vm.$nextTick();

  t.is(stateChangeEventData.jobId, 'foo', 'data should be matched');
  t.is(stateChangeEventData.metricId, 'bar', 'data should be matched');
  t.is(stateChangeEventData.metricType, 'JobMetric', 'data should be matched');
  t.is(stateChangeEventData.prevState, 'READY', 'data should be matched');
  t.is(stateChangeEventData.newState, 'EXECUTING', 'data should be matched');

  t.is(dagData.dag, 'bar-dag', 'data should be matched');
  t.is(dagData.jobId, 'foo', 'data should be matched');
  t.false(dagData.init, 'data should be matched');

  vm.$eventBus.$off('state-change-event');
  vm.$eventBus.$off('dag');
  vm.$destroy();
});
