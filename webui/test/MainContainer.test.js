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
import MainContainer from '../components/MainContainer';
import { METRIC_LIST } from '../components/MainContainer';

test.before('initialize', async t => {
  // load all plugins
  require('../plugins/event-bus');
});

test.serial('beforeMount', async t => {
  const vm = new Vue(MainContainer).$mount();

  t.is(vm.groupDataSet.length, METRIC_LIST.length,
    'groupDataSet should be built with METRIC_LIST');

  vm.$destroy();
});

test.serial('job id event handler', async t => {
  const vm = new Vue(MainContainer).$mount();
  let setTimelineItemsCounter = 0;
  let clearStageSelectCounter = 0;

  vm.$eventBus.$on('set-timeline-items', data => {
    setTimelineItemsCounter++;
  });

  vm.$eventBus.$emit('job-id-select', {
    jobId: 'foo',
    jobFrom: 'bar',
    metricLookupMap: 'lorem',
    metricDataSet: 'ipsum',
  });
  await vm.$nextTick();

  t.is(setTimelineItemsCounter, 1,
    'set-timeline-items event should be fired');
  t.is(vm.selectedJobId, 'foo', 'job id should be changed correctly');
  t.is(vm.jobFrom, 'bar', 'jobFrom should be changed correctly');
  t.is(vm.metricLookupMap, 'lorem',
    'metricLookupMap should be changed correctly');

  vm.$eventBus.$emit('job-id-deselect');
  await vm.$nextTick();

  t.is(setTimelineItemsCounter, 2,
    'set-timeline-items event should be fired again');
  t.is(vm.selectedJobId, '', 'job id should be cleared');
  t.is(vm.jobFrom, '', 'jobFrom should be cleared');
  t.deepEqual(vm.metricLookupMap, {}, 'metricLookupMap should be cleared');

  vm.$destroy();
});

test.serial('table data event handler', async t => {
  const vm = new Vue(MainContainer).$mount();
  let tableData = '';

  vm.buildTableData = (data) => { tableData = data; };

  vm.selectedMetricId = 'lorem';
  vm.selectedJobId = 'ipsum';

  vm.$eventBus.$emit('build-table-data', {
    metricId: 'foo',
    jobId: 'bar',
  });
  await vm.$nextTick();

  t.is(tableData, '',
    'buildTableData should not be called id is different');

  vm.$eventBus.$emit('build-table-data', {
    metricId: 'lorem',
    jobId: 'ipsum',
  });
  await vm.$nextTick();

  t.is(tableData, 'lorem', 'buildTableData should be called');

  vm.$destroy();
});

test.serial('metric selection event handler', async t => {
  const vm = new Vue(MainContainer).$mount();
  let metricSelectDoneCounter = 0;
  let metricDeselectDoneCounter = 0;

  vm.$eventBus.$on('metric-select-done', () => {
    metricSelectDoneCounter++;
  });

  vm.$eventBus.$on('metric-deselect-done', () => {
    metricDeselectDoneCounter++;
  });

  vm.buildTableData = (data) => {
    t.is(data, 'foo', 'metric-select should be deliver correct metric id');
  };

  vm.$eventBus.$emit('metric-select', 'foo');
  await vm.$nextTick();

  t.is(vm.selectedMetricId, 'foo',
    'metric-select event should change selected metric id');
  t.is(metricSelectDoneCounter, 1,
    'metric-select event should fire metric-select-done event');

  vm.$eventBus.$emit('metric-deselect');
  await vm.$nextTick();

  t.deepEqual(vm.tableData, [],
    'metric-deselect event should clear tableData');
  t.is(vm.selectedMetricId, '',
    'metric-deselect event should clear selectedMetricId');
  t.is(metricDeselectDoneCounter, 1,
    'metric-deselect event should fire metric-deselect-done event');

  vm.$eventBus.$off('metric-select-done');
  vm.$eventBus.$off('metric-deselect-done');
  vm.$destroy();
});

test.serial('buildTableData functionality', t => {
  const vm = new Vue(MainContainer).$mount();

  vm.metricLookupMap = {
    fooMetric: {
      group: 'dog',
      content: 'cat',

      executionProperties: {
        epFoo: 'epFooValue',
      },

      fooProperty: -1,
      barProperty: 'ruff ruff',
    }
  };

  vm.buildTableData('fooMetric');
  t.is(3, vm.tableData.length);

  for (const item of vm.tableData) {
    switch (item.key) {
      case 'executionProperties':
        t.truthy('extra' in item,
          'extra field should valid in executionProperties data');
        t.is(item.extra[0].key, 'epFoo',
          'executionProperties should be built correctly');
        t.is(item.extra[0].value, 'epFooValue',
          'executionProperties should be built correctly');
        break;
      case 'fooProperty':
        t.is(item.value, 'N/A', '-1 should be converted to N/A');
        break;
      case 'barProperty':
        t.is(item.value, 'ruff ruff', 'other values should be kept');
        break;
      case 'group':
      case 'content':
        t.fail('`group` or `content` property should be discarded');
        break;
    }
  }

  vm.$destroy();
});
