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
import StageSelect from '../components/StageSelect';

test.before('initialize', async t => {
  // load all plugins
  require('../plugins/event-bus');
});

test.serial('correctly filter stage list', async t => {
  const vm = new Vue(StageSelect);

  vm.metricLookupMap = {
    'Stagefoo': 'X',
    'Stage0': 'O',
    'Stage1234': 'O',
    'Stage1234-10-5678': 'X',
    'fooStage1': 'X',
  };
  vm.$mount();

  t.is(vm.stageList.length, 2, 'stageList should be correctly built');
  t.is(vm.stageList.sort()[0], 'Stage0',
    'stageList should be correctly built')
  t.is(vm.stageList.sort()[1], 'Stage1234',
    'stageList should be correctly built')

  vm.$destroy();
});

test.serial('event handlers', async t => {
  const vm = new Vue(StageSelect);
  vm.metricLookupMap = {};
  vm.$mount();

  let filterAndSendData = null;

  vm.filterAndSend = (data) => {
    filterAndSendData = data;
  };

  vm.filterItem = () => true;
  const jobId = 'foo-job';
  vm.selectedJobId = jobId;

  const item = {
    id: 'foo',
    content: 'foo content',
  };

  const newItem = {
    id: 'foo',
    content: 'bar content',
    type: 'range'
  };

  vm.$eventBus.$emit('set-timeline-items', 'lorem');
  await vm.$nextTick();

  t.is(filterAndSendData, 'lorem',
    'set-timeline-items event should call filterAndSend');

  vm.$eventBus.$emit('add-timeline-item', {
    jobId,
    item,
  });
  await vm.$nextTick();

  t.deepEqual(vm.newMetricDataSet.get('foo'), item,
    'add-timeline-item event should add item to inner DataSet');

  vm.$eventBus.$emit('update-timeline-item', {
    jobId,
    item: newItem,
  });
  await vm.$nextTick();

  t.deepEqual(vm.newMetricDataSet.get('foo'), newItem,
    'update-timeline-item event should update item of inner DataSet');

  vm.selectData = ['foo', 'bar'];
  vm.$eventBus.$emit('clear-stage-select');
  await vm.$nextTick();

  t.deepEqual(vm.selectData, [],
    'clear-stage-select event should clear selected data');

  vm.$destroy();
});
