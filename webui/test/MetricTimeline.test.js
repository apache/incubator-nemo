import test from 'ava';
import Vue from 'vue';
import MetricTimeline from '../components/MetricTimeline';
import { FIT_THROTTLE_INTERVAL } from '../components/MetricTimeline';

function wait(interval) {
  return new Promise(resolve => {
    setTimeout(resolve, interval);
  });
};

test.before('initialize', async t => {
  // load all plugins
  require('../plugins/event-bus');
});

test.serial('redraw-timeline event', async t => {
  const vm = new Vue(MetricTimeline);
  vm.$refs.timeline = {
    on() {},
  };
  vm.$mount();

  let redrawTimelineCounter = 0;

  vm.redrawTimeline = () => {
    redrawTimelineCounter++;
  };

  vm.$eventBus.$emit('redraw-timeline');
  await vm.$nextTick();

  t.is(redrawTimelineCounter, 1,
    'redraw-timeline event should call redrawTimeline');

  t.falsy(vm.fitThrottlerTimer,
    'fitThrottlerTimer should be null initially');

  vm.$destroy();
});

test.serial('move-timeline event', async t => {
  const vm = new Vue(MetricTimeline);
  vm.$refs.timeline = {
    on() {},
  };
  vm.$mount();

  let moveTimelineCounter = 0;

  vm.moveTimeline = () => {
    moveTimelineCounter++;
  };

  vm.selectedJobId = 'foo';

  vm.$eventBus.$emit('move-timeline', {
    jobId: 'bar',
    time: 'time',
  });
  await vm.$nextTick();

  t.is(moveTimelineCounter, 0,
    'move-timeline event should be ignored when job id is not equal');

  vm.$eventBus.$emit('move-timeline', {
    jobId: 'foo',
    time: 'time',
  });
  await vm.$nextTick();

  t.is(moveTimelineCounter, 1,
    'move-timeline event should call moveTimeline');

  vm.$destroy();
});

test.serial('fit-timeline event', async t => {
  const vm = new Vue(MetricTimeline);
  vm.$refs.timeline = {
    on() {},
  };
  vm.$mount();

  let fitTimelineCounter = 0;

  vm.fitTimeline = () => {
    fitTimelineCounter++;
  };

  vm.selectedJobId = 'foo';

  vm.$eventBus.$emit('fit-timeline', 'bar');
  await vm.$nextTick();
  await wait(FIT_THROTTLE_INTERVAL * 2);
  t.is(fitTimelineCounter, 0,
    'fit-timeline event should be ignored when job id is not equal');

  vm.$eventBus.$emit('fit-timeline', 'foo');
  await vm.$nextTick();
  t.not(vm.fitThrottlerTimer, null,
    'fit-timeline event should start timer');
  t.is(fitTimelineCounter, 0,
    'fitTimeline function call should be throttled');
  await wait(FIT_THROTTLE_INTERVAL * 2);
  t.is(fitTimelineCounter, 1,
    'fitTimeline should be called after FIT_THROTTLE_INTERVAL');

  vm.$destroy();
});
