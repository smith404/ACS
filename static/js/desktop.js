angular.module('app').controller('MainController', function() {
    this.patternData = {
        identifier: 'Pattern-1',
        slices: [
            { distribution: 0.5, startDistribution: 0.2, duration: 100, startOffset: 10, durationOffset: 20, developmentPeriods: 5 },
            { distribution: 0.3, startDistribution: 0.1, duration: 100, startOffset: 30, durationOffset: 20, developmentPeriods: 5 }
        ],
        duration: 100
    };
    this.maxSlice = this.patternData.slices.length;
    this.selectedSlice = 0;

    this.onSliceChange = function() {
        this.maxSlice = this.patternData.slices.length;
        console.log('Slice changed. New maxSlice:', this.maxSlice);
    };

    this.onSelectedSliceChange = function(selectedSlice) {
        this.selectedSlice = selectedSlice;
        console.log('Selected slice changed:', this.selectedSlice);
    };
});