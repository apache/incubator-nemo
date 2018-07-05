import Vue from 'vue';
import ReconnectingWebSocket from '~/assets/reconnecting-websocket';

export default ({ app, store }) => {
  const ws = new ReconnectingWebSocket('ws://localhost:10101/api/websocket');
  ws.reconnectInterval = 1000;
  ws.maxReconnectInterval = 1000;

  ws.onopen = (event) => {
    store.commit('clearMetric');
    store.commit('setWebSocket', ws);
    console.log('Connected!');
  }

  ws.onmessage = async (event) => {
    let parsedData;
    try {
      parsedData = JSON.parse(event.data);
    } catch (e) {
      console.warn('Non-JSON data received');
      return;
    }

    await store.dispatch('processMetric', parsedData);
  }

};
