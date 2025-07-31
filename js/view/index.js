define(['backbone', 'resthub', 'hbs!template/index'], function(Backbone, Resthub, indexTemplate) {

  var IndexView = Resthub.View.extend({
    root: '#app',
    template: indexTemplate,
    className: 'hero-unit'
  });

  return IndexView;

});
