angular.module('app').controller('PatternController', function($http) {
  let ctrl = this;

  ctrl.onSliderChange = function() {
    ctrl.onSelectedSliceChange({ selectedSlice: ctrl.selectedSlice });
  };

  ctrl.onViewModelChange = function() {
    document.getElementById('svgOtherContainer').innerHTML = ctrl[ctrl.viewMode + 'SVG'];
  };

  ctrl.$onInit = function() {
    ctrl.showWritten = true;
    ctrl.showUnwritten = false;
    ctrl.showLIC = false;
    ctrl.showLRC = false;
    ctrl.showUPR = false;
    ctrl.showText = false;
    ctrl.selectedSlice = 0;

    // Placeholder for SVG images
    ctrl.fullSVG = "";
    ctrl.writtenSVG = "";
    ctrl.unwrittenSVG = "";
    ctrl.licSVG = "";
    ctrl.lrcSVG = "";
    ctrl.uprSVG = "";
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

  ctrl.generateSvgs = function() {
    const patternTypes = ['full', 'written', 'unwritten', 'lic', 'lrc', 'upr'];
    for (const type of patternTypes) {
      ctrl.generateSvg(type);
    }
  };

  ctrl.generateSvg = function(patternType) {
    let patternBlocks = ctrl.getPatternBlocks();
    let url = '/svg/generate?type=' + patternType + "&lw=" + ctrl.selectedSlice;
    url = ctrl.showText ? url + '?text=true' : url;
    $http.post(url, { patternBlocks: patternBlocks }).then(function(response) {
      ctrl[patternType + 'SVG'] = response.data;
      if (patternType === 'full') {
        document.getElementById('svgFullContainer').innerHTML = ctrl[patternType + 'SVG'];
      }
      if (patternType === ctrl.viewMode) {
        document.getElementById('svgOtherContainer').innerHTML = ctrl[patternType + 'SVG'];
      }
    }).catch(function(error) {
      console.error('Error generating SVG:', error);
    });
  };
});
