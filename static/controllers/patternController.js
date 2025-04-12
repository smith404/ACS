angular.module('app').controller('PatternController', function($http) {
  let ctrl = this;

  ctrl.onSliderChange = function() {
    ctrl.generateSvgs();
    ctrl.onSelectedElementChange({ selectedElement: ctrl.selectedElement });
  };

  ctrl.onViewModelChange = function() {
    document.getElementById('svgOtherContainer').innerHTML = ctrl[ctrl.viewMode + 'SVG'];
  };

  ctrl.$onInit = function() {
    ctrl.selectedElement = 0;
    ctrl.ultimateValue = 0;
    ctrl.viewMode = 'written';

    // Placeholder for SVG images
    ctrl.fullSVG = "";
    ctrl.writtenSVG = "";
    ctrl.unwrittenSVG = "";
    ctrl.licSVG = "";
    ctrl.lrcSVG = "";
    ctrl.uprSVG = "";

    ctrl.patternValuesByTimeElement = [];
    ctrl.cumulativePatternValuesByTimeElement = [];
  };

  ctrl.addElement = function(element) {
    ctrl.patternData.elements.push(element);
    ctrl.alignElementPeriods();
    ctrl.onElementChange();
  };

  ctrl.removeElement = function(index) {
    ctrl.patternData.elements.splice(index, 1);
    ctrl.alignElementPeriods();
    ctrl.onElementChange();
  };

  ctrl.setDuration = function(duration) {
    ctrl.patternData.duration = duration;
    ctrl.patternData.patternData.duration = duration;
    ctrl.patternData.elements.forEach(function(element) {
      element.patternData.duration = duration;
    });
  };

  ctrl.alignElementPeriods = function(developmentPeriods = null) {
    if (developmentPeriods === null || developmentPeriods === 0) {
      developmentPeriods = ctrl.patternData.elements.length;
    }
    ctrl.patternData.elements.forEach((element, index) => {
      element.developmentPeriods = developmentPeriods;
      element.weight = (1 / ctrl.patternData.elements.length);
      element.durationOffset = ctrl.patternData.duration / developmentPeriods;
      element.startOffset = index * (ctrl.patternData.duration / developmentPeriods);
    });
  };

  ctrl.distributeRemaining = function() {
    let totalDistribution = ctrl.patternData.elements.reduce((sum, element) => sum + element.distribution + element.startDistribution, 0);
    if (totalDistribution < 1) {
      let remaining = 1 - totalDistribution;
      ctrl.patternData.elements.forEach(element => {
        element.distribution += (remaining / ctrl.patternData.elements.length);
      });
    }
  };

  ctrl.checkDistribution = function() {
    let totalDistribution = ctrl.patternData.elements.reduce((sum, element) => sum + element.distribution + element.startDistribution, 0);
    let result = totalDistribution === 1;
    alert("Distribution check: " + (result ? "Valid" : "Invalid"));
  };

  ctrl.clearStartDistributions = function() {
    ctrl.patternData.elements.forEach(element => {
      element.startDistribution = 0;
    });
  };

  ctrl.clearDistributions = function() {
    ctrl.patternData.elements.forEach(element => {
      element.startDistribution = 0;
      element.distribution = 0;
    });
  };

  ctrl.getPatternBlocks = function() {

    let blocks = [];
    let displayLevel = 0;
    ctrl.patternData.elements.forEach((element, index) => {
      blocks = blocks.concat(ctrl.getElementBlocks(element, ctrl.patternData.identifier, index, displayLevel));
      if (element.startDistribution && element.startDistribution !== 0) {
        displayLevel += 1;
      }
      if (element.distribution && element.distribution !== 0) {
        displayLevel += 1;
      }
    });
    return blocks;
  };

  ctrl.getElementBlocks = function(element, patternId, elementNumber, displayLevel) {
    let blocks = [];
    if (element.startDistribution && element.startDistribution !== 0) {
      for (let index = 0; index < element.developmentPeriods; index++) {
        let shape = 'FIRST';
        let startPoint = element.startOffset + (index * element.durationOffset);
        let endPoint = element.startOffset + ((index + 1) * element.durationOffset) - 1;
        let block = {
          pattern: patternId,
          elementNumber: elementNumber,
          displayLevel: displayLevel,
          startPoint: startPoint,
          endPoint: endPoint,
          proportion: element.startDistribution / element.developmentPeriods,
          value: ctrl.ultimateValue * (element.startDistribution / element.developmentPeriods),
          shape: shape
        };
        blocks.push(block);
      }
      displayLevel += 1;
    }
    if (element.distribution && element.distribution !== 0) {
      for (let index = 0; index <= element.developmentPeriods; index++) {
        let shape = 'LINEAR';
        let factor = element.developmentPeriods;
        if (index === 0 || index === element.developmentPeriods) {
          factor *= 2;
          shape = index === 0 ? 'INC_PROP' : 'DEC_PROP';
        }
        let startPoint = element.startOffset + (index * element.durationOffset);
        let endPoint = element.startOffset + ((index + 1) * element.durationOffset) - 1;
        let block = {
          pattern: patternId,
          elementNumber: elementNumber,
          displayLevel: displayLevel,
          startPoint: startPoint,
          endPoint: endPoint,
          proportion: element.distribution / factor,
          value: ctrl.ultimateValue * (element.distribution / factor),
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
    let url = '/svg/generate?type=' + patternType + "&lw=" + ctrl.selectedElement;
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

    ctrl.patternValuesByTimeElement = ctrl.sumValuesByElementEndPoint(blocks);    
    ctrl.cumulativePatternValuesByTimeElement = ctrl.getCumulativeValues(ctrl.patternValuesByTimeElement);

    let selectedElement = ctrl.selectedElement;
    let esp = ctrl.getEarliestStartPointOfElement(blocks, selectedElement);

    ctrl.writtenValue = ctrl.evaluateWrittenBlocks(blocks, selectedElement).toFixed(4);
    ctrl.unwrittenValue = ctrl.evaluateUnwrittenBlocks(blocks, selectedElement).toFixed(4);
    ctrl.lic = ctrl.evaluateLICBlocks(blocks, selectedElement, esp).toFixed(4);
    ctrl.lrc = ctrl.evaluateLRCBlocks(blocks, selectedElement, esp).toFixed(4);
    ctrl.upr = ctrl.evaluateUPRBlocks(blocks, esp).toFixed(4);
  } 

  ctrl.getEarliestStartPointOfElement = function(blocks, elementNumber) {
    if (elementNumber >= ctrl.patternData.elements.length) {
      return ctrl.getEarliestEndPointOfElement(blocks, elementNumber - 1) + 1;
    }
    else {
      let elementBlocks = blocks.filter(block => block.elementNumber === elementNumber);
      return elementBlocks.reduce((min, block) => Math.min(min, block.startPoint), Number.MAX_VALUE);
      }
  };

  ctrl.getEarliestEndPointOfElement = function(blocks, elementNumber) {
    let elementBlocks = blocks.filter(block => block.elementNumber === elementNumber);
    return elementBlocks.reduce((min, block) => Math.min(min, block.endPoint), Number.MAX_VALUE) + 1;
  };

  ctrl.evaluateWrittenBlocks = function(blocks, elementNumber) {
    let filterBlocks = blocks.filter(block => block.elementNumber < elementNumber);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateUnwrittenBlocks = function(blocks, elementNumber) {
    let filterBlocks = blocks.filter(block => block.elementNumber >= elementNumber);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateLICBlocks = function(blocks, elementNumber, esp) {
    let filterBlocks = blocks.filter(block => block.elementNumber < elementNumber && block.endPoint < esp);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateLRCBlocks = function(blocks, elementNumber, esp) {
    let filterBlocks = blocks.filter(block => block.elementNumber < elementNumber && block.endPoint >= esp);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.evaluateUPRBlocks = function(blocks, esp) {
    let filterBlocks = blocks.filter(block => block.endPoint >= esp);
    return filterBlocks.reduce((sum, block) => sum + block.value, 0);
  };

  ctrl.sumValuesByElementEndPoint = function(blocks) {
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
