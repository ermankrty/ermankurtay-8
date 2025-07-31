define(['backbone', 'collection/tasks', 'model/task', 'view/tasks'], function(Backbone, Tasks, Task, TasksView) {

  var TasksRouter = Backbone.Router.extend({
    routes: {
      '':                'index',
      'tasks':           'list',
      'task/new':        'create',
      'task/:id':        'show',
      'task/:id/edit':   'edit',
      'task/:id/remove': 'remove'
    },

    initialize: function() {
      this.tasks = new Tasks([{
        id: 1,
        title: 'Task1',
        description: 'Task1 desc.'
      }, {
        id: 2,
        title: 'Task2',
        description: 'Task2 desc.'
      }]);
    },

    index: function() {
      require(['view/index'], function(IndexView) {
        var indexView = new IndexView();
        indexView.render();
      });
    },

    list: function() {
      if (!this.tasksView || this.tasksView.$el.parent().size() === 0) {
        this.tasksView = new TasksView({collection: this.tasks});
        this.tasksView.render();
      }

      if (this.currentView) {
        this.currentView.remove();
      }

      this.active();
    },

    active: function(activeTask) {
      if (this.activeTask) {
        this.activeTask.set('active', false);
      }
      if (activeTask) {
        this.activeTask = activeTask;
        this.activeTask.set('active', true);
      }
    },

    current: function(view, id) {
      var task = this.tasks.get(id);
      if (!task) {
        alert('The task with id ' + id + ' does not exist.');
        return this.navigate('#tasks', true);
      }

      this.list();
      this.active(task);

      require(['view/' + view], _.bind(function(viewClass) {
        this.currentView = new viewClass({model: task});
        this.currentView.render();
      }, this));
    },

    create: function() {
      var task = new Task({id: this.tasks.last().get('id') + 1});
      this.tasks.add(task);
      this.current('task-form', task.get('id'));
    },

    show: function(id) {
      this.current('task-details', id);
    },

    edit: function(id) {
      this.current('task-form', id);
    },

    remove: function(id) {
      if (confirm('Do you really want to remove "' + this.tasks.get(id).get('title') + '"?')) {
        this.tasks.remove(this.tasks.get(id));
        this.navigate('#tasks', true);
      } else {
        this.navigate('#task/' + id + '/edit', true);
      }
    }
  });

  return TasksRouter;

});
