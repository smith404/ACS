angular.module('app').controller('PatternController', function($http) {
  let ctrl = this;

  ctrl.onSliderChange = function() {
    ctrl.dump()
    ctrl.generateSvgs();
    ctrl.onSelectedSliceChange({ selectedSlice: ctrl.selectedSlice });
  };

  ctrl.onViewModelChange = function() {
    document.getElementById('svgOtherContainer').innerHTML = ctrl[ctrl.viewMode + 'SVG'];
  };

  ctrl.$onInit = function() {
    ctrl.selectedSlice = 0;
    ctrl.ultimateValue = 0;
    ctrl.viewMode = 'written';

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
          value: ctrl.ultimateValue * (slice.startDistribution / slice.developmentPeriods),
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
          value: ctrl.ultimateValue * (slice.distribution / factor),
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
    url = ctrl.showText ? url + '&text=true' : url;
    if (patternType !== 'full') {
      url = url + '&val=true';
    }
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

  ctrl.dump = function()
  {
    let blocks = ctrl.getPatternBlocks();
    let selectedSlice = ctrl.selectedSlice;
    let esp = ctrl.getEarliestStartPointOfSlice(blocks, selectedSlice);

    console.log("SS: " + selectedSlice);
    console.log("ESP: " + ctrl.getEarliestStartPointOfSlice(blocks, selectedSlice));
    console.log("Written: " + ctrl.evaluateWrittenBlocks(blocks, selectedSlice));
    console.log("Unwritten: " + ctrl.evaluateUnwrittenBlocks(blocks, selectedSlice));
    console.log("LIC: " + ctrl.evaluateLICBlocks(blocks, selectedSlice, esp));
    console.log("LRC: " + ctrl.evaluateLRCBlocks(blocks, selectedSlice, esp));
    console.log("UPR: " + ctrl.evaluateUPRBlocks(blocks, esp));
  } 

  ctrl.getEarliestStartPointOfSlice = function(blocks, sliceNumber) {
    if (sliceNumber >= ctrl.patternData.slices.length) {
      return ctrl.getEarliestEndPointOfSlice(blocks, sliceNumber - 1) + 1;
    }
    else {
      let sliceBlocks = blocks.filter(block => block.sliceNumber === sliceNumber);
      return sliceBlocks.reduce((min, block) => Math.min(min, block.startPoint), Number.MAX_VALUE);
      }
  };

  ctrl.getEarliestEndPointOfSlice = function(blocks, sliceNumber) {
    let sliceBlocks = blocks.filter(block => block.sliceNumber === sliceNumber);
    return sliceBlocks.reduce((min, block) => Math.min(min, block.endPoint), Number.MAX_VALUE) + 1;
  };

  ctrl.evaluateWrittenBlocks = function(blocks, sliceNumber) {
    let filterBlocks = blocks.filter(block => block.sliceNumber < sliceNumber);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateUnwrittenBlocks = function(blocks, sliceNumber) {
    let filterBlocks = blocks.filter(block => block.sliceNumber >= sliceNumber);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateLICBlocks = function(blocks, sliceNumber, esp) {
    let filterBlocks = blocks.filter(block => block.sliceNumber < sliceNumber && block.endPoint < esp);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateLRCBlocks = function(blocks, sliceNumber, esp) {
    let filterBlocks = blocks.filter(block => block.sliceNumber < sliceNumber && block.endPoint >= esp);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateUPRBlocks = function(blocks, esp) {
    let filterBlocks = blocks.filter(block => block.endPoint >= esp);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };
});
