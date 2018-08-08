import test from 'ava';
import Vue from 'vue';
import DetailTable from '../components/DetailTable';

test.before('initialize', async t => {
  // load all plugins
  require('../plugins/event-bus');
});

test.serial('key preprocessing', async t => {
  const vm = new Vue(DetailTable).$mount();

  const key1 = 'foo.bar';
  const key2 = 'foobar';

  const newKey1 = vm._preprocessKey(key1);
  t.is(newKey1, 'bar', 'should split key by dot');

  const newKey2 = vm._preprocessKey(key2);
  t.is(newKey2, 'foobar',
    'should return original key if there is no dot');

  vm.$destroy();
});

test.serial('row class name', async t => {
  const vm = new Vue(DetailTable).$mount();

  const rowObject1 = {
    row: {
      key: 'foo',
      value: 'bar',
    }
  };
  t.is(vm._rowClassName(rowObject1), 'no-expand',
    'should return no-expand if extra field does not exist');

  const rowObject2 = {
    row: {
      key: 'foo',
      value: 'bar',
      extra: 'baz,'
    }
  };
  t.is(vm._rowClassName(rowObject2), '',
    'should return empty string if extra field exists');

  vm.$destroy();
});
