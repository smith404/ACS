angular.module('app').component('pattern', {
  bindings: {
    patternData: '=',
    onSliceChange: '&',
    onSelectedSliceChange: '&'
  },
  controller: function($http) {
    let ctrl = this;
    
    ctrl.onSliderChange = function() {
      ctrl.onSelectedSliceChange({ selectedSlice: ctrl.selectedSlice });
    };

    ctrl.selectedSlice = 0;

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

    ctrl.alignSlicePeriods = function(developmentPeriods = null) {
      if (developmentPeriods === null || developmentPeriods === 0) {
        developmentPeriods = ctrl.patternData.slices.length;
      }
      ctrl.patternData.slices.forEach((slice, index) => {
        slice.developmentPeriods = developmentPeriods;
        slice.durationOffset = ctrl.patternData.duration / developmentPeriods;
        slice.startOffset = index * (ctrl.patternData.duration / developmentPeriods);
      });
    };

    ctrl.distributeRemaining = function() {
      let totalDistribution = ctrl.patternData.slices.reduce((sum, slice) => sum + slice.distribution + slice.startDistribution, 0);
      if (totalDistribution < 1) {
        let remaining = 1 - totalDistribution;
        ctrl.patternData.slices.forEach(slice => {
          slice.distribution += remaining / ctrl.patternData.slices.length;
        });
      }
    };

    ctrl.checkDistribution = function() {
      let totalDistribution = ctrl.patternData.slices.reduce((sum, slice) => sum + slice.distribution + slice.startDistribution, 0);
      console.log(totalDistribution);
      let result = totalDistribution === 1;
      alert("Distribution check: " + (result ? "Valid" : "Invalid"));
    };

    ctrl.clearStartDistributions = function() {
      ctrl.patternData.slices.forEach(slice => {
        slice.startDistribution = 0;
      });
  };

    ctrl.clearDistributions = function() {
      ctrl.patternData.slices.forEach(slice => {
        slice.startDistribution = 0;
        slice.distribution = 0;
      });
    };

    ctrl.getPatternBlocks = function() {
      let blocks = [];
      let displayLevel = 0;
      ctrl.alignSlicePeriods();
      ctrl.patternData.slices.forEach((slice, index) => {
        blocks = blocks.concat(ctrl.getSliceBlocks(slice, ctrl.patternData.identifier, index, displayLevel));
        displayLevel += slice.startDistribution !== 0 ? 2 : 1;
      });
      return blocks;
    };

    ctrl.getSliceBlocks = function(slice, patternId, sliceNumber, displayLevel) {
      let blocks = [];
      if (slice.startDistribution !== 0) {
        for (let index = 0; index < slice.developmentPeriods; index++) {
          let shape = 'RECTANGLE';
          let startPoint = slice.startOffset + (index * slice.durationOffset);
          let endPoint = slice.startOffset + ((index + 1) * slice.durationOffset) - 1;
          let block = {
            pattern: patternId,
            sliceNumber: sliceNumber,
            displayLevel: displayLevel,
            startPoint: startPoint,
            endPoint: endPoint,
            height: slice.startDistribution / slice.developmentPeriods,
            shape: shape
          };
          blocks.push(block);
        }
        displayLevel += 1;
      }
      if (slice.distribution !== 0) {
        for (let index = 0; index <= slice.developmentPeriods; index++) {
          let shape = 'RECTANGLE';
          let factor = slice.developmentPeriods;
          if (index === 0 || index === slice.developmentPeriods) {
            factor *= 2;
            shape = index === 0 ? 'LTRIANGLE' : 'RTRIANGLE';
          }
          let startPoint = slice.startOffset + (index * slice.durationOffset);
          let endPoint = slice.startOffset + ((index + 1) * slice.durationOffset) - 1;
          let block = {
            pattern: patternId,
            sliceNumber: sliceNumber,
            displayLevel: displayLevel,
            startPoint: startPoint,
            endPoint: endPoint,
            height: slice.distribution / factor,
            shape: shape
          };
          blocks.push(block);
        }
      }
      return blocks;
    };

    ctrl.generateSvg = function() {
      let patternBlocks = ctrl.getPatternBlocks();
      $http.post('/svg/generate', { patternBlocks: patternBlocks }).then(function(response) {
        ctrl.svgContent = response.data;
        document.getElementById('svgContainer').innerHTML = ctrl.svgContent;
      }).catch(function(error) {
        console.error('Error generating SVG:', error);
      });
    };
  },
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
      <ul class="nav nav-tabs" id="myTab" role="tablist">
          <li class="nav-item" role="presentation">
              <button class="nav-link active" id="tab1-tab" data-bs-toggle="tab" data-bs-target="#tab0" type="button" role="tab" aria-controls="tab1" aria-selected="true">Base</button>
          </li>
          <li class="nav-item" role="presentation">
              <button class="nav-link" id="tab1-tab" data-bs-toggle="tab" data-bs-target="#tab1" type="button" role="tab" aria-controls="tab1" aria-selected="true">Written</button>
          </li>
          <li class="nav-item" role="presentation">
              <button class="nav-link" id="tab2-tab" data-bs-toggle="tab" data-bs-target="#tab2" type="button" role="tab" aria-controls="tab2" aria-selected="false">Unwritten</button>
          </li>
          <li class="nav-item" role="presentation">
              <button class="nav-link" id="tab3-tab" data-bs-toggle="tab" data-bs-target="#tab3" type="button" role="tab" aria-controls="tab3" aria-selected="false">LIC</button>
          </li>
          <li class="nav-item" role="presentation">
              <button class="nav-link" id="tab4-tab" data-bs-toggle="tab" data-bs-target="#tab4" type="button" role="tab" aria-controls="tab4" aria-selected="false">LRC</button>
          </li>
          <li class="nav-item" role="presentation">
              <button class="nav-link" id="tab5-tab" data-bs-toggle="tab" data-bs-target="#tab5" type="button" role="tab" aria-controls="tab5" aria-selected="false">UPR</button>
          </li>
      </ul>
      <div class="tab-content" id="myTabContent">
          <div class="tab-pane fade show active" id="tab0" role="tabpanel" aria-labelledby="tab1-tab">
            <div id="svgContainer" style="width: 20%;"></div>
          </div>
          <div class="tab-pane fade" id="tab1" role="tabpanel" aria-labelledby="tab1-tab">
              <img width="400" height="400" src="/svg/pattern/my_test_pattern?type=written&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="tab-pane fade" id="tab2" role="tabpanel" aria-labelledby="tab2-tab">
              <img width="400" height="400" src="/svg/pattern/my_test_pattern?type=unwritten&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="tab-pane fade" id="tab3" role="tabpanel" aria-labelledby="tab3-tab">
              <img width="400" height="400" src="/svg/pattern/my_test_pattern?type=lic&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="tab-pane fade" id="tab4" role="tabpanel" aria-labelledby="tab4-tab">
              <img width="400" height="400" src="/svg/pattern/my_test_pattern?type=lrc&lw=1" alt="Pattern {{ pattern }}">
          </div>
          <div class="tab-pane fade" id="tab5" role="tabpanel" aria-labelledby="tab5-tab">
              <img width="400" height="400" src="/svg/pattern/my_test_pattern?type=upr&lw=1" alt="Pattern {{ pattern }}">
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
      </div>
    </div>
  `
});