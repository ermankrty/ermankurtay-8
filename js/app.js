require(['backbone', 'collection/tasks', 'router/tasks'], function(Backbone, Tasks, TasksRouter) {

  new TasksRouter();
  Backbone.history.start();

});
