import test from 'ava';
import Vue from 'vue';
import TaskStatistics from '../components/TaskStatistics';
import { EXCLUDE_COLUMN } from '../components/TaskStatistics';

test.before('initialize', async t => {
  // load all plugins
  require('../plugins/event-bus');
});

test.serial('preprocessing metric', async t => {
  const vm = new Vue(TaskStatistics);
  vm.metricLookupMap = {
    fooMetric: {
      group: 'TaskMetric',
      fooProperty: -1,
      barProperty: 'bar',
    },
  };
  vm.$mount();

  t.is(vm.taskMetric[0].fooProperty, 'N/A',
    'should convert NOT_AVAILABLE constant to N/A');

  vm.$destroy();
});

test.serial('column generation', async t => {
  const vm = new Vue(TaskStatistics);
  vm.metricLookupMap = {
    fooMetric: {
      fooProperty: -1,
      barProperty: 'bar',
    },
  };

  EXCLUDE_COLUMN.forEach(c => {
    vm.metricLookupMap.fooMetric[c] = 'foo ' + c;
  });
  vm.metricLookupMap.fooMetric.group = 'TaskMetric';

  vm.$mount();

  t.is(vm.columnArray.length, 2, 'should discard specific columns');
  t.truthy(vm.columnArray.includes('fooProperty'));
  t.truthy(vm.columnArray.includes('barProperty'));

  vm.$destroy();
});
