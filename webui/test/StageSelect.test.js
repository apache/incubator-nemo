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
    'Stage-foo': 'X',
    'Stage-0': 'O',
    'Stage-1234': 'O',
    'Stage-1234-Task-5678': 'X',
    'fooStage-1': 'X',
  };
  vm.$mount();

  t.is(vm.stageList.length, 2, 'stageList should be correctly built');
  t.is(vm.stageList.sort()[0], 'Stage-0',
    'stageList should be correctly built')
  t.is(vm.stageList.sort()[1], 'Stage-1234',
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
