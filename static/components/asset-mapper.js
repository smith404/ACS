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
  templateUrl: '/static/components/templates/asset-mapper.html'
});

angular.module('app').component('transform', {
  controller: 'TransformController',
  templateUrl: '/static/components/templates/transform.html'
});

