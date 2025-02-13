angular.module('app').component('patternSlice', {
  bindings: {
    distribution: '<',
    startDistribution: '<',
    duration: '<',
    startOffset: '<',
    durationOffset: '<',
    developmentPeriods: '<'
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

    ctrl.iterateDevelopmentPeriods = function() {
      var result = [];
      for (var index = 0; index <= ctrl.developmentPeriods; index++) {
        var factor = ctrl.developmentPeriods;
        if (index === 0 || index === ctrl.developmentPeriods) {
          factor = factor * 2;
        }
        result.push({
          start: ctrl.startOffset + (index * ctrl.durationOffset),
          end: (ctrl.startOffset + ((index + 1) * ctrl.durationOffset)) - 1,
          value: ctrl.distribution / factor
        });
      }
      return result;
    };

    ctrl.iterateStartPeriods = function() {
      var result = [];
      var factor = ctrl.developmentPeriods;
      for (var index = 0; index < ctrl.developmentPeriods; index++) {
        result.push({
          start: ctrl.startOffset + (index * ctrl.durationOffset),
          end: (ctrl.startOffset + ((index + 1) * ctrl.durationOffset)) - 1,
          value: ctrl.startDistribution / factor
        });
      }
      return result;
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