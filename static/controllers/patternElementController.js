angular.module('app').controller('PatternElementController', function() {
  let ctrl = this;

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
});
