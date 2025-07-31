define(['backbone', 'resthub', 'view/task', 'hbs!template/tasks'], function(Backbone, Resthub, TaskView, tasksTemplate) {

  var TasksView = Resthub.View.extend({
    root: '#app',
    template: tasksTemplate,
    id: 'tasks',
    initialize: function() {
      this.listenTo(this.collection, 'reset', this.render);
      this.listenTo(this.collection, 'add', this.add);
    },
    render: function() {
      console.log('TasksView rendering...');
      TasksView.__super__.render.apply(this);
      this.collection.each(this.add, this);
      return this;
    },
    add: function(task) {
      var taskView = new TaskView({model: task, tagName: 'li'});
      this.$('li').last().before(taskView.render().el);
    }
  });

  return TasksView;

});
