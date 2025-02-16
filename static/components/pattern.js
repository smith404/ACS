angular.module('app').component('pattern', {
  bindings: {
    patternData: '=',
    onSliceChange: '&',
    onSelectedSliceChange: '&'
  },
  controller: 'PatternController',
  template: `
    <div class="card">
      <div class="card-header">
        <div class="form-group row align-items-center">
          <label class="col-sm-1 col-form-label">Pattern</label>
          <div class="col-sm-4">
            <input type="text" ng-model="$ctrl.patternData.identifier" class="form-control" />
          </div>
          <label class="col-sm-2 col-form-label">Default Duration</label>
          <div class="col-sm-1">
            <input type="number" ng-model="$ctrl.patternData.duration" class="form-control" />
          </div>
          <label class="col-sm-2 col-form-label">Latest Written Slice: {{$ctrl.selectedSlice}}</label>
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
                  start-distribution="slice.startDistribution" 
                  duration="slice.duration" 
                  start-offset="slice.startOffset" 
                  duration-offset="slice.durationOffset" 
                  development-periods="slice.developmentPeriods">
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
        <div id="diagrams" class="row justify-content-center" style="max-height: 50vh;">
          <div class="col" ng-if="$ctrl.showBase">
            <h3>Base</h3>
            <div id="svgContainer"></div>
          </div>
          <div class="col" ng-if="$ctrl.showWritten">
            <h3>Written</h3>
            <img src="/svg/pattern/my_test_pattern?type=written&lw=1&text=true" alt="Pattern {{ pattern }}">
          </div>
          <div class="col" ng-if="$ctrl.showUnwritten">
            <h3>Unwritten</h3>
            <img src="/svg/pattern/my_test_pattern?type=unwritten&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="col" ng-if="$ctrl.showLIC">
            <h3>LIC</h3>
            <img src="/svg/pattern/my_test_pattern?type=lic&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="col" ng-if="$ctrl.showLRC">
            <h3>LRC</h3>
            <img src="/svg/pattern/my_test_pattern?type=lrc&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="col" ng-if="$ctrl.showUPR">
            <h3>UPR</h3>
            <img src="/svg/pattern/my_test_pattern?type=upr&lw=1" alt="Pattern {{ pattern }}">
          </div>
        </div>
      </div>
      <div class="card-footer">
        <button class="btn btn-pond" ng-click="$ctrl.addSlice({distribution: 0, startDistribution: 0, duration: $ctrl.patternData.duration, startOffset: 0, durationOffset: 0, developmentPeriods: 0})" title="Add Slice">
          <i class="fas fa-plus-square"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.distributeRemaining()" title="Distribute Remaining">
          <i class="fas fa-chart-bar"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.generateSvg()" title="Generate Blocks">
          <i class="fas fa-cubes"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.clearDistributions()" title="Clear Distributions">
          <i class="fas fa-broom"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.checkDistribution()" title="Check Pattern">
          <i class="fas fa-check-square"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.savePattern()" title="Save">
          <i class="fas fa-save"></i>
        </button>
        <label>
          <input type="checkbox" ng-model="$ctrl.showBase">
          Show Base
        </label>
        <label>
          <input type="checkbox" ng-model="$ctrl.showWritten">
          Show Written
        </label>
      </div>
    </div>
  `
});