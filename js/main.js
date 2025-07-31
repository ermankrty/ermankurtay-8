require.config({
  shim: {
    'underscore': {
      exports: '_'
    },
    'handlebars': {
      exports: 'Handlebars'
    },
    'backbone': {
      deps: [
        'underscore',
        'jquery'
      ],
      exports: 'Backbone'
    }
  },

  paths: {
    text:       'lib/text',
    hbs:        'lib/resthub/require-handlebars',
    jquery:     'lib/jquery',
    underscore: 'lib/underscore',
    backbone:   'lib/backbone',
    handlebars: 'lib/handlebars',
    resthub:    'lib/resthub/resthub'
  }
});

require(['app']);
