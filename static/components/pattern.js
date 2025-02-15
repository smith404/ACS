angular.module('app').component('pattern', {
  bindings: {
    patternData: '=',
    onSliceChange: '&',
    onSelectedSliceChange: '&'
  },
  controller: function() {
    let ctrl = this;
    
    ctrl.onSliderChange = function() {
      ctrl.onSelectedSliceChange({ selectedSlice: ctrl.selectedSlice });
    };

    ctrl.selectedSlice = 0;

    ctrl.$onInit = function() {
    };

    ctrl.addSlice = function(slice) {
      console.log(slice);
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
    <div class="card">
      <div class="card-header">
        <div class="form-group row">
          <label class="col-sm-1 col-form-label">Pattern</label>
          <div class="col-sm-4">
            <input type="text" ng-model="$ctrl.patternData.identifier" class="form-control" />
          </div>
          <label class="col-sm-1 col-form-label">Duration</label>
          <div class="col-sm-2">
            <input type="number" ng-model="$ctrl.patternData.duration" class="form-control" />
          </div>
          <label class="col-sm-2 col-form-label">Written Slice {{$ctrl.selectedSlice}}</label>
          <div class="col-sm-2">
            <input type="range" id="sliceRange" min="0" max="{{$ctrl.patternData.slices.length}}" ng-model="$ctrl.selectedSlice" ng-change="$ctrl.onSliderChange()">
          </div>
        </div>
      </div>
      <div class="card-body">
        <ul class="nav nav-tabs" id="patternTabs" role="tablist">
          <li class="nav-item" role="presentation" ng-repeat="slice in $ctrl.patternData.slices">
            <a class="nav-link" id="slice-tab-{{$index}}" data-bs-toggle="tab" href="#slice{{$index}}" role="tab" aria-controls="slice{{$index}}" aria-selected="{{$index === 0}}">Slice: {{$index + 1}}</a>
          </li>
        </ul>
        <div class="tab-content" id="patternTabContent">
          <div class="tab-pane fade" id="slice{{$index}}" role="tabpanel" aria-labelledby="slice-tab-{{$index}}" ng-class="{show: $index === 0, active: $index === 0}" ng-repeat="slice in $ctrl.patternData.slices">
            <div class="row">
              <div class="col-sm-11">
                <pattern-slice 
                  distribution="slice.distribution" 
                  start-distribution="slice.start_distribution" 
                  duration="slice.duration" 
                  start-offset="slice.start_offset" 
                  duration-offset="slice.duration_offset" 
                  development-periods="slice.development_periods">
                </pattern-slice>
              </div>
              <div class="col-sm-1 d-flex align-items-center">
                <button class="btn btn-pond" ng-click="$ctrl.removeSlice($index)" title="Remove Slice" ng-disabled="$ctrl.patternData.slices.length === 1">
                  <i class="fas fa-minus-square"></i>
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="card-footer">
        <button class="btn btn-pond" ng-click="$ctrl.addSlice({distribution: 0, start_distribution: 0, duration: $ctrl.patternData.duration, startOffset: 0, duration_offset: 0, development_periods: 0})" title="Add Slice">
          <i class="fas fa-plus-square"></i>
        </button>
      </div>
    </div>
  `
});