angular.module('app').component('pattern', {
  bindings: {
    patternData: '=',
    onElementChange: '&',
    onSelectedElementChange: '&'
  },
  controller: 'PatternController',
  template: `
    <div class="row mt-3">
      <div class="col-12">
        <div class="form-group row align-items-center">
          <label class="col-sm-1 col-form-label">Pattern</label>
          <div class="col-sm-4">
            <input type="text" ng-model="$ctrl.patternData.identifier" class="form-control" />
          </div>
          <label class="col-sm-2 col-form-label">Default Duration</label>
          <div class="col-sm-1">
            <input type="number" ng-model="$ctrl.patternData.duration" class="form-control" />
          </div>
          <label class="col-sm-2 col-form-label">Latest Written Element: {{$ctrl.selectedElement}}</label>
          <div class="col-sm-2">
            <input type="range" id="elementRange" min="0" max="{{$ctrl.patternData.elements.length}}" ng-model="$ctrl.selectedElement" ng-change="$ctrl.onSliderChange()">
          </div>
        </div>
      </div>
    </div>
    <div class="row mt-3 mb-3">
      <div class="col-12">
        <button class="btn btn-pond" ng-click="$ctrl.addElement({distribution: 0, startDistribution: 0, duration: $ctrl.patternData.duration, startOffset: 0, durationOffset: 0, developmentPeriods: 0, weight: 0})" title="Add Element">
          <i class="fas fa-plus-square"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.distributeRemaining()" title="Distribute Remaining">
          <i class="fas fa-chart-bar"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.generateSvgs()" title="Generate Blocks">
          <i class="fas fa-cubes"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.clearDistributions()" title="Clear Distributions">
          <i class="fas fa-broom"></i>
        </button>
        <button class="btn btn-pond" ng-click="$ctrl.checkDistribution()" title="Check Pattern">
          <i class="fas fa-check-square"></i>
        </button>
        <label class="form-switch ms-3">
          <input type="radio" ng-model="$ctrl.viewMode" value="written" class="form-check-input" ng-change="$ctrl.onViewModelChange()">
          Show Written
        </label>
        <label class="form-switch ms-3">
          <input type="radio" ng-model="$ctrl.viewMode" value="unwritten" class="form-check-input" ng-change="$ctrl.onViewModelChange()">
          Show Unwritten
        </label>
        <label class="form-switch ms-3">
          <input type="radio" ng-model="$ctrl.viewMode" value="lic" class="form-check-input" ng-change="$ctrl.onViewModelChange()">
          Show LIC
        </label>
        <label class="form-switch ms-3">
          <input type="radio" ng-model="$ctrl.viewMode" value="lrc" class="form-check-input" ng-change="$ctrl.onViewModelChange()">
          Show LRC
        </label>
        <label class="form-switch ms-3">
          <input type="radio" ng-model="$ctrl.viewMode" value="upr" class="form-check-input" ng-change="$ctrl.onViewModelChange()">
          Show UPR
        </label>
        <label class="form-switch ms-3">
          <input type="checkbox" ng-model="$ctrl.showText" class="form-check-input">
          Show Text
        </label>
      </div>
    </div>
    <div id="pattern-elements" class="row mt-1 border border-secondary" ng-repeat="element in $ctrl.patternData.elements">
      <div class="col-sm-11">
        <pattern-element 
          distribution="element.distribution" 
          start-distribution="element.startDistribution" 
          duration="element.duration" 
          weight="element.weight"
          start-offset="element.startOffset" 
          duration-offset="element.durationOffset" 
          development-periods="element.developmentPeriods">
        </pattern-element>
      </div>
      <div class="col-sm-1 d-flex align-items-center">
        <button class="btn btn-pond" ng-click="$ctrl.removeElement($index)" title="Remove Element" ng-disabled="$ctrl.patternData.elements.length === 1">
          <i class="fas fa-minus-square"></i>
        </button>
      </div>
    </div>
    <div class="row mt-3">
      <div class="col">
        <h4>Base</h4>
        <div id="svgFullContainer"></div>
      </div>
      <div class="col">
        <h4>Comparison</h4>
        <div id="svgOtherContainer"></div>
      </div>
    </div>
    <div class="row mt-3 justify-content-md-center">
      <div class="col">
        <label class="form-switch ms-3">
          <input type="number" ng-model="$ctrl.ultimateValue" class="form-control" placeholder="Ultimate Value">
        </label>
      </div>
      <div class="col">
        <label class="form-switch ms-3">
          Written Value: {{$ctrl.writtenValue}}
        </label>
      </div>
      <div class="col">
        <label class="form-switch ms-3">
          Unwritten Value: {{$ctrl.unwrittenValue}}
        </label>
      </div>
      <div class="col">
        <label class="form-switch ms-3">
          LIC: {{$ctrl.lic}}
        </label>
      </div>
      <div class="col">
        <label class="form-switch ms-3">
          LRC: {{$ctrl.lrc}}
        </label>
      </div>
      <div class="col">
        <label class="form-switch ms-3">
          UPR: {{$ctrl.upr}}
        </label>
      </div>
    </div>
    <div class="row mt-3 justify-content-md-center">
      <div class="col">
        <table class="table table-striped mt-3">
          <thead>
            <tr>
              <th>End Point</th>
              <th>Value</th>
              <th>Cumulative Value</th>
            </tr>
          </thead>
          <tbody>
            <tr ng-repeat="value in $ctrl.cumulativePatternValuesByTimeElement">
              <td>{{ value.endPoint + 1 }}</td>
              <td>{{ value.value }}</td>
              <td>{{ value.cumulativeValue }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  `
});