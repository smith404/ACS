angular.module('app').component('pattern', {
  bindings: {
    patternData: '=',
    onSliceChange: '&'
  },
  controller: function() {
    let ctrl = this;

    ctrl.$onInit = function() {
    };

    ctrl.addSlice = function(slice) {
      ctrl.patternData.slices.push(slice);
      ctrl.onSliceChange();
    };

    ctrl.removeSlice = function(index) {
      ctrl.patternData.slices.splice(index, 1);
      ctrl.onSliceChange();
    };

    ctrl.setDuration = function(duration) {
      ctrl.patternData.duration = duration;
      ctrl.patternData.patternData.duration = duration;
      ctrl.patternData.slices.forEach(function(slice) {
        slice.patternData.duration = duration;
      });
    };
  },
  template: `
    <div>
      <h3>Pattern: {{$ctrl.patternData.identifier}}</h3>
      <p>Duration: {{$ctrl.patternData.duration}}</p>
      <ul>
        <li ng-repeat="slice in $ctrl.patternData.slices">
          <pattern-slice 
            distribution="slice.distribution" 
            start-distribution="slice.startDistribution" 
            duration="slice.duration" 
            start-offset="slice.startOffset" 
            duration-offset="slice.durationOffset" 
            development-periods="slice.developmentPeriods">
          </pattern-slice>
          <button class="btn btn-sm btn-pond" ng-click="$ctrl.removeSlice($index)"><i class="fas fa-bread-slice"></i><i class="fas fa-minus-square" style="margin-left: 15px;"></i></button>
        </li>
      </ul>
      <button class="btn btn-sm btn-pond" ng-click="$ctrl.addSlice({distribution: 0, startDistribution: 0, duration: $ctrl.duration, startOffset: 0, durationOffset: 0, developmentPeriods: 0})">
        <i class="fas fa-bread-slice"></i><i class="fas fa-plus-square" style="margin-left: 15px;"></i>
      </button>
    </div>
  `
});