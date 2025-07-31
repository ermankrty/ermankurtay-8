define(['backbone', 'resthub', 'hbs!template/task'], function(Backbone, Resthub, taskTemplate) {

  var TaskView = Resthub.View.extend({
    template: taskTemplate,
    initialize: function() {
      this.listenTo(this.model, 'change', this.render);
      this.listenTo(this.model, 'remove', this.remove);
    },
    render: function() {
      console.log('TaskView rendering: ' + this.model.get('title'));
      if (this.model.get('active')) {
        this.$el.addClass('active');
      } else {
        this.$el.removeClass('active');
      }
      TaskView.__super__.render.apply(this);
      return this;
    }
  });

  return TaskView;

});
