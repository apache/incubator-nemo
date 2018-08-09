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

  t.is(vm.columnArray.length, 3, 'should discard specific columns');
  t.truthy(vm.columnArray.includes('duration'), 'should add duration field');
  t.truthy(vm.columnArray.includes('fooProperty'));
  t.truthy(vm.columnArray.includes('barProperty'));

  vm.$destroy();
});
