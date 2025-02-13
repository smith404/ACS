angular.module('app').component('sliceSelector', {
  bindings: {
    maxSlice: '<',
    selectedSlice: '='
  },
  controller: function() {
    var ctrl = this;

    ctrl.$onInit = function() {
      if (ctrl.selectedSlice === undefined) {
        ctrl.selectedSlice = 0;
      }
    };

    ctrl.onSliderChange = function() {
      // Handle slider change if needed
    };
  },
  template: `
    <div>
      <label for="sliceRange">Select Slice: {{$ctrl.selectedSlice}}</label>
      <input type="range" id="sliceRange" min="0" max="{{$ctrl.maxSlice}}" ng-model="$ctrl.selectedSlice" ng-change="$ctrl.onSliderChange()">
    </div>
  `
});