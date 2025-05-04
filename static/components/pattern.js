angular.module('app').component('pattern', {
  bindings: {
    patternData: '=',
    onElementChange: '&',
    onSelectedElementChange: '&'
  },
  controller: 'PatternController',
  templateUrl: '/static/components/templates/pattern.html'
});