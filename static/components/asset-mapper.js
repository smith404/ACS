angular.module('app').component('assetMapper', { // Renamed from assetMapping to assetMapper
  bindings: {
    fromAsset: '=',
    toAsset: '=',
    keyValuePairs: '='
  },
  controller: function () {
    this.addKeyValuePair = function () {
      this.keyValuePairs.push({ key: '', value: '' });
    };

    this.removeKeyValuePair = function (index) {
      this.keyValuePairs.splice(index, 1);
    };
  },
  template: `
    <div class="row mt-3">
      <div class="col-6">
        <label>From Asset</label>
        <input type="text" ng-model="$ctrl.fromAsset" class="form-control" />
      </div>
      <div class="col-6">
        <label>To Asset</label>
        <input type="text" ng-model="$ctrl.toAsset" class="form-control" />
      </div>
    </div>
    <div class="row mt-3">
      <div class="col-12">
        <h5>Key-Value Pairs</h5>
        <table class="table table-bordered">
          <thead>
            <tr>
              <th>Key</th>
              <th>Value</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            <tr ng-repeat="pair in $ctrl.keyValuePairs">
              <td><input type="text" ng-model="pair.key" class="form-control" /></td>
              <td><input type="text" ng-model="pair.value" class="form-control" /></td>
              <td>
                <button class="btn btn-danger" ng-click="$ctrl.removeKeyValuePair($index)">
                  <i class="fas fa-trash"></i>
                </button>
              </td>
            </tr>
          </tbody>
        </table>
        <button class="btn btn-primary" ng-click="$ctrl.addKeyValuePair()">
          <i class="fas fa-plus"></i> Add Pair
        </button>
      </div>
    </div>
  `
});
