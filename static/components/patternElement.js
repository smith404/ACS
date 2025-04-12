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
  template: `
    <div class="form-group row align-items-center">
      <label class="col-sm-1 col-form-label">Start Distribution:</label>
      <div class="col-sm-1">
        <input type="number" ng-model="$ctrl.startDistribution" class="form-control" />
      </div>
      <label class="col-sm-1 col-form-label">Distribution:</label>
      <div class="col-sm-1">
        <input type="number" ng-model="$ctrl.distribution" class="form-control" />
      </div>
      <label class="col-sm-1 col-form-label">Weight:</label>
      <div class="col-sm-1">
        <input type="number" ng-model="$ctrl.weight" class="form-control" />
      </div>
      <label class="col-sm-1 col-form-label">
      Start {{$ctrl.startOffset}}
      </label>
      <label class="col-sm-1 col-form-label">
      End {{$ctrl.startOffset + $ctrl.durationOffset}}
      </label>
      <label class="col-sm-1 col-form-label">Development Periods:</label>
      <div class="col-sm-1">
        <input type="number" ng-model="$ctrl.developmentPeriods" class="form-control" />
      </div>
    </div>
  `
});