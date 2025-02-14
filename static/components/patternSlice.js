angular.module('app').component('patternSlice', {
  bindings: {
    distribution: '=',
    startDistribution: '=',
    duration: '=',
    startOffset: '=',
    durationOffset: '=',
    developmentPeriods: '='
  },
  controller: function() {
    var ctrl = this;

    ctrl.$onInit = function() {
      // Initialization logic
    };

    ctrl.setDuration = function(duration) {
      ctrl.duration = duration;
    };

    ctrl.setStartOffset = function(startOffset) {
      ctrl.startOffset = startOffset;
    };

    ctrl.setDurationOffset = function(durationOffset) {
      ctrl.durationOffset = durationOffset;
    };

    ctrl.setDevelopmentPeriods = function(developmentPeriods) {
      ctrl.developmentPeriods = developmentPeriods;
    };
  },
  template: `
    <div>
      <h3>Pattern Slice</h3>
      <p>Distribution: {{$ctrl.distribution}}</p>
      <p>Start Distribution: {{$ctrl.startDistribution}}</p>
      <p>Duration: {{$ctrl.duration}}</p>
      <p>Start Offset: {{$ctrl.startOffset}}</p>
      <p>Duration Offset: {{$ctrl.durationOffset}}</p>
      <p>Development Periods: {{$ctrl.developmentPeriods}}</p>
    </div>
  `
});