angular.module('app').controller('AssetMapperController', function () {
    this.addKeyValuePair = function () {
        this.keyValuePairs.push({ key: '', value: '' });
      };
  
    this.removeKeyValuePair = function (index) {
        this.keyValuePairs.splice(index, 1);
    };
});
  