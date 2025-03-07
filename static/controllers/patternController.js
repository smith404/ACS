angular.module('app').controller('PatternController', function($http) {
  let ctrl = this;

  ctrl.onSliderChange = function() {
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

    ctrl.patternValuesByTimeSlice = [];
    ctrl.cumulativePatternValuesByTimeSlice = [];
  };

  ctrl.addSlice = function(slice) {
    ctrl.patternData.slices.push(slice);
    ctrl.alignSlicePeriods();
    ctrl.onSliceChange();
  };

  ctrl.removeSlice = function(index) {
    ctrl.patternData.slices.splice(index, 1);
    ctrl.alignSlicePeriods();
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
      slice.weight = (1 / ctrl.patternData.slices.length);
      slice.durationOffset = ctrl.patternData.duration / developmentPeriods;
      slice.startOffset = index * (ctrl.patternData.duration / developmentPeriods);
    });
  };

  ctrl.distributeRemaining = function() {
    let totalDistribution = ctrl.patternData.slices.reduce((sum, slice) => sum + slice.distribution + slice.startDistribution, 0);
    if (totalDistribution < 1) {
      let remaining = 1 - totalDistribution;
      ctrl.patternData.slices.forEach(slice => {
        slice.distribution += (remaining / ctrl.patternData.slices.length);
      });
    }
  };

  ctrl.checkDistribution = function() {
    let totalDistribution = ctrl.patternData.slices.reduce((sum, slice) => sum + slice.distribution + slice.startDistribution, 0);
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
    ctrl.patternData.slices.forEach((slice, index) => {
      blocks = blocks.concat(ctrl.getSliceBlocks(slice, ctrl.patternData.identifier, index, displayLevel));
      if (slice.startDistribution && slice.startDistribution !== 0) {
        displayLevel += 1;
      }
      if (slice.distribution && slice.distribution !== 0) {
        displayLevel += 1;
      }
    });
    return blocks;
  };

  ctrl.getSliceBlocks = function(slice, patternId, sliceNumber, displayLevel) {
    let blocks = [];
    if (slice.startDistribution && slice.startDistribution !== 0) {
      for (let index = 0; index < slice.developmentPeriods; index++) {
        let shape = 'FIRST';
        let startPoint = slice.startOffset + (index * slice.durationOffset);
        let endPoint = slice.startOffset + ((index + 1) * slice.durationOffset) - 1;
        let block = {
          pattern: patternId,
          sliceNumber: sliceNumber,
          displayLevel: displayLevel,
          startPoint: startPoint,
          endPoint: endPoint,
          proportion: slice.startDistribution / slice.developmentPeriods,
          value: ctrl.ultimateValue * (slice.startDistribution / slice.developmentPeriods),
          shape: shape
        };
        blocks.push(block);
      }
      displayLevel += 1;
    }
    if (slice.distribution && slice.distribution !== 0) {
      for (let index = 0; index <= slice.developmentPeriods; index++) {
        let shape = 'LINEAR';
        let factor = slice.developmentPeriods;
        if (index === 0 || index === slice.developmentPeriods) {
          factor *= 2;
          shape = index === 0 ? 'INC_PROP' : 'DEC_PROP';
        }
        let startPoint = slice.startOffset + (index * slice.durationOffset);
        let endPoint = slice.startOffset + ((index + 1) * slice.durationOffset) - 1;
        let block = {
          pattern: patternId,
          sliceNumber: sliceNumber,
          displayLevel: displayLevel,
          startPoint: startPoint,
          endPoint: endPoint,
          proportion: slice.distribution / factor,
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
    ctrl.calculate() 
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

  ctrl.calculate = function()
  {
    let blocks = ctrl.getPatternBlocks();

    ctrl.patternValuesByTimeSlice = ctrl.sumValuesBySliceEndPoint(blocks);    
    ctrl.cumulativePatternValuesByTimeSlice = ctrl.getCumulativeValues(ctrl.patternValuesByTimeSlice);

    let selectedSlice = ctrl.selectedSlice;
    let esp = ctrl.getEarliestStartPointOfSlice(blocks, selectedSlice);

    ctrl.writtenValue = ctrl.evaluateWrittenBlocks(blocks, selectedSlice).toFixed(4);
    ctrl.unwrittenValue = ctrl.evaluateUnwrittenBlocks(blocks, selectedSlice).toFixed(4);
    ctrl.lic = ctrl.evaluateLICBlocks(blocks, selectedSlice, esp).toFixed(4);
    ctrl.lrc = ctrl.evaluateLRCBlocks(blocks, selectedSlice, esp).toFixed(4);
    ctrl.upr = ctrl.evaluateUPRBlocks(blocks, esp).toFixed(4);
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

  ctrl.sumValuesBySliceEndPoint = function(blocks) {
    let sumByEndPoint = {};

    blocks.forEach(block => {
      if (!sumByEndPoint[block.endPoint]) {
        sumByEndPoint[block.endPoint] = 0;
      }
      sumByEndPoint[block.endPoint] += block.value;
    });

    ctrl.getCumulativeValues(sumByEndPoint);
    return sumByEndPoint;
  };

  ctrl.getCumulativeValues = function(sumValues) {
    let cumulativeValues = [];
    let cumulativeSum = 0;

    Object.keys(sumValues).sort((a, b) => a - b).forEach(endPoint => {
      cumulativeSum += sumValues[endPoint];
      cumulativeValues.push({ 
        endPoint: parseInt(endPoint), 
        cumulativeValue: parseFloat(cumulativeSum.toFixed(4)),
        value: parseFloat(sumValues[endPoint].toFixed(4))
      });
    });

    return cumulativeValues;
  };
});
