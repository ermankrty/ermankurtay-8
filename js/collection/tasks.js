define(['backbone', 'model/task'], function(Backbone, Task) {

  var Tasks = Backbone.Collection.extend({
    model: Task
  });

  return Tasks;

});
