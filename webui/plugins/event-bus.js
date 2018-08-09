import Vue from 'vue';

const eventBus = {};

eventBus.install = ( Vue ) => {
  Vue.prototype.$eventBus = new Vue();
};

Vue.use(eventBus);
