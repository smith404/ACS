angular.module('app').component('patternElement', {
  bindings: {
    distribution: '=',
    startDistribution: '=',
    duration: '=',
    weight: '=',
    startOffset: '=',
    durationOffset: '=',
    developmentPeriods: '='
  },
  controller: 'PatternElementController',
  templateUrl: '/static/components/templates/pattern-element.html'
});