angular.module('app').component('patternSlice', {
  bindings: {
    distribution: '=',
    startDistribution: '=',
    duration: '=',
    startOffset: '=',
    durationOffset: '=',
    developmentPeriods: '='
  },
  controller: 'PatternSliceController',
  template: `
    <div class="form-group row align-items-center">
      <label class="col-sm-1 col-form-label">Start Distribution</label>
      <div class="col-sm-3">
        <input type="number" ng-model="$ctrl.startDistribution" class="form-control" />
      </div>
      <label class="col-sm-1 col-form-label">Distribution</label>
      <div class="col-sm-3">
        <input type="number" ng-model="$ctrl.distribution" class="form-control" />
      </div>
      <label class="col-sm-1 col-form-label">Duration</label>
      <div class="col-sm-3">
        <input type="number" ng-model="$ctrl.duration" class="form-control" />
      </div>
    </div>
  `
});