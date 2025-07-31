define(['backbone', 'resthub', 'hbs!template/task-form'], function(Backbone, Resthub, taskFormTemplate) {

  var TaskFormView = Resthub.View.extend({
    root: '.task',
    template: taskFormTemplate,
    events: {
      'submit form': 'save'
    },
    save: function() {
      this.model.set({
        title: this.$('#title').val(),
        description: this.$('#description').val()
      });
      //this.model.save();
      Backbone.history.navigate("#task/" + this.model.get('id'), true);
      return false;
    }
  });

  return TaskFormView;

});
