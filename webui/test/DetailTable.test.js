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
