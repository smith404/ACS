angular.module('app').controller('MainController', ['$http', function($http) {
    let ctrl = this;

    ctrl.loadPatternData = function() {
        $http.get('/pattern/load/my_test_pattern')
            .then(function(response) {
                ctrl.patternData = response.data;
                ctrl.maxSlice = ctrl.patternData.slices.length;
            }, function(error) {
                console.error('Error loading pattern data:', error);
            });
    };

    ctrl.initPatternData = function() {
        $http.get('/pattern/new')
            .then(function(response) {
                ctrl.patternData = response.data;
                ctrl.maxSlice = ctrl.patternData.slices.length;
            }, function(error) {
                console.error('Error creating pattern:', error);
            });
    };
    ctrl.onSliceChange = function() {
        ctrl.maxSlice = ctrl.patternData.slices.length;
        console.log('Slice changed. New maxSlice:', ctrl.maxSlice);
    };

    ctrl.onSelectedSliceChange = function(selectedSlice) {
        ctrl.selectedSlice = selectedSlice;
        console.log('Selected slice changed:', ctrl.selectedSlice);
    };

    ctrl.selectedSlice = 0;
    ctrl.initPatternData();

    
}]);