angular.module('app').controller('TransformController', function () {
  this.types = ['map', 'rename', 'drop', 'simplemap', 'select', 'group by'];
  this.type = this.types[0]; // Default value
});
