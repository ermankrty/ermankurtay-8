define(['backbone', 'resthub', 'hbs!template/task-details'], function(Backbone, Resthub, taskDetailsTemplate) {

  var TaskDetailsView = Resthub.View.extend({
    root: '.task',
    template: taskDetailsTemplate,
    initialize: function() {
      this.listenTo(this.model, 'change', this.render);
    },
    render: function() {
      console.log('TaskDetailsView rendering: ' + this.model.get('title'));
      TaskDetailsView.__super__.render.apply(this);
    }
  });

  return TaskDetailsView;

});
