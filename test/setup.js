const hooks = require('require-extension-hooks');

require('babel-polyfill');
hooks('vue').plugin('vue').push();
hooks(['vue', 'js']).plugin('babel').push();
