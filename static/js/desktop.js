angular.module('app').controller('MainController', ['$http', function($http) {
    let ctrl = this;

    ctrl.loadPatternData = function() {
        $http.get('/pattern/load/my_test_pattern')
            .then(function(response) {
                ctrl.patternData = response.data;
                ctrl.maxElement = ctrl.patternData.elements.length;
            }, function(error) {
                console.error('Error loading pattern data:', error);
            });
    };

    ctrl.initPatternData = function() {
        $http.get('/pattern/new')
            .then(function(response) {
                ctrl.patternData = response.data;
                ctrl.maxElement = ctrl.patternData.elements.length;
            }, function(error) {
                console.error('Error creating pattern:', error);
            });
    };
    ctrl.onElementChange = function() {
        ctrl.maxElement = ctrl.patternData.elements.length;
    };

    ctrl.onSelectedElementChange = function(selectedElement) {
        ctrl.selectedElement = selectedElement;
    };

    ctrl.selectedElement = 0;
    ctrl.initPatternData();

    
}]);