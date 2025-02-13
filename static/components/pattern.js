angular.module('app').component('pattern', {
  bindings: {
    patternData: '<',
    onSliceChange: '&'
  },
  controller: function() {
    var ctrl = this;

    ctrl.$onInit = function() {
      ctrl.identifier = ctrl.patternData.identifier;
      ctrl.slices = ctrl.patternData.slices;
      ctrl.duration = ctrl.patternData.duration;
    };

    ctrl.addSlice = function(slice) {
      ctrl.slices.push(slice);
      ctrl.onSliceChange();
    };

    ctrl.removeSlice = function(index) {
      ctrl.slices.splice(index, 1);
      ctrl.onSliceChange();
    };

    ctrl.setDuration = function(duration) {
      ctrl.duration = duration;
      ctrl.slices.forEach(function(slice) {
        slice.duration = duration;
      });
    };
  },
  template: `
    <div>
      <h3>Pattern: {{$ctrl.identifier}}</h3>
      <p>Duration: {{$ctrl.duration}}</p>
      <ul>
        <li ng-repeat="slice in $ctrl.slices">
          <pattern-slice 
            distribution="slice.distribution" 
            start-distribution="slice.startDistribution" 
            duration="slice.duration" 
            start-offset="slice.startOffset" 
            duration-offset="slice.durationOffset" 
            development-periods="slice.developmentPeriods">
          </pattern-slice>
          <button class="btn btn-pond" ng-click="$ctrl.removeSlice($index)">Remove Slice</button>
        </li>
      </ul>
      <button class="btn btn-pond" ng-click="$ctrl.addSlice({distribution: 0, startDistribution: 0, duration: $ctrl.duration, startOffset: 0, durationOffset: 0, developmentPeriods: 0})">Add Slice</button>
    </div>
  `
});