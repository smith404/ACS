angular.module('app').component('assetMapper', { // Renamed from assetMapping to assetMapper
  bindings: {
    fromAsset: '=',
    toAsset: '=',
    keyValuePairs: '='
  },
  controller: 'AssetMapperController',
  templateUrl: '/static/components/templates/asset-mapper.html'
});

angular.module('app').component('transform', {
  controller: 'TransformController',
  templateUrl: '/static/components/templates/transform.html'
});

