# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from flask import Flask, send_from_directory, render_template, Response, request
from flask_wtf.csrf import CSRFProtect
from pattern import Pattern
from pattern_evaluator import PatternBlock, PatternEvaluator
from pattern_slice import PatternSlice

app = Flask(__name__)
#csrf = CSRFProtect(app)

@app.route('/static/<path:filename>')
def static_files(filename: str) -> Response:
    return send_from_directory('static', filename)

@app.route('/')
@app.route('/desktop/')
def desktop() -> str:
    return render_template('desktop.html')

@app.route('/svg/generate', methods=['POST'])
def generate_svg() -> Response:
    data = request.json
    if not data or 'patternBlocks' not in data:
        return Response("Invalid data", status=400)
    
    svgType = request.args.get('type', 'full')
    latestWrittenSlice = int(request.args.get('lw', 0))
    showText = request.args.get('text', 'false').lower() == 'true'
    showValue = request.args.get('val', 'false').lower() == 'true'

    patternBlocks = [PatternBlock(**block) for block in data['patternBlocks']]
    evaluator = PatternEvaluator(patternBlocks)

    if svgType == 'written':
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, condition="t", showText=showText, showValue=showValue)
    elif svgType == 'unwritten':
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, condition="b", showText=showText, showValue=showValue)
    elif svgType == 'lic':
        timePoint = evaluator.get_earliest_start_point_of_slice(latestWrittenSlice)
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, dayCut=timePoint, condition="tl", showText=showText, showValue=showValue)
    elif svgType == 'lrc':
        timePoint = evaluator.get_earliest_start_point_of_slice(latestWrittenSlice)
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, dayCut=timePoint, condition="tr", showText=showText, showValue=showValue)
    elif svgType == 'upr':
        timePoint = evaluator.get_earliest_start_point_of_slice(latestWrittenSlice)
        svgContent = evaluator.create_svg(evaluator.patternBlocks, dayCut=timePoint, showText=showText, condition="r", showValue=showValue)
    else:
        svgContent = evaluator.create_svg(evaluator.patternBlocks, colour='white', showText=showText, showValue=showValue)

    return Response(svgContent, mimetype='image/svg+xml')

@app.route('/pattern/new')
def pattern_new() -> Response:
    pattern = Pattern()
    slice = PatternSlice()
    slice.developmentPeriods = 1
    slice.weight = 1
    pattern.add_slice(slice)
    return Response(pattern.to_json(), mimetype='application/json')

@app.route('/patternslice/new')
def pattern_slice__new() -> Response:
    patternSlice = PatternSlice()
    return Response(patternSlice.to_json(), mimetype='application/json')

@app.route('/patternblock/new')
def pattern_block_new() -> Response:
    patternBlock = PatternBlock("New Pattern")
    return Response(patternBlock.to_json(), mimetype='application/json')

@app.route('/pattern/load/<name>')
def pattern_object(name: str) -> Response:
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    return Response(pattern.to_json(), mimetype='application/json')

@app.route('/pattern/save', methods=['POST'])
def save_pattern() -> Response:
    data = request.json
    if not data:
        return Response("Invalid data", status=400)
    
    pattern = Pattern.from_json(data)
    pattern.save_to_file(f'scratch/{pattern.identifier}.json')
    return Response("Pattern saved successfully", status=200)

@app.route('/svg/pattern/<name>')
def pattern_svg(name: str) -> Response:
    pattern = Pattern.load_from_file(f'scratch/{name}.json')
    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    svgType = request.args.get('type', 'full')
    latestWrittenSlice = int(request.args.get('lw', 0))
    showText = request.args.get('text', 'false').lower() == 'true'

    svgContent = generate_svg_content(evaluator, svgType, latestWrittenSlice, showText=showText)
    return Response(svgContent, mimetype='image/svg+xml')

def generate_svg_content(evaluator: PatternEvaluator, svgType: str, latestWrittenSlice: int, showText: bool = True) -> str:
    if svgType == 'written':
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, preColour='lightblue', colour='white', showText=showText)
    elif svgType == 'unwritten':
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, showText=showText)
    elif svgType == 'lic':
        timePoint = evaluator.get_earliest_end_point_of_slice(latestWrittenSlice)
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, dayCut=timePoint, preColour='lightblue', colour='white', condition="and", showText=showText)
    elif svgType == 'lrc':
        timePoint = evaluator.get_earliest_end_point_of_slice(latestWrittenSlice)
        return evaluator.create_svg(evaluator.patternBlocks, latestWrittenSlice=latestWrittenSlice, dayCut=timePoint, preColour='white', colour='lightblue', condition="and", showText=showText)
    elif svgType == 'upr':
        timePoint = evaluator.get_earliest_end_point_of_slice(latestWrittenSlice)
        return evaluator.create_svg(evaluator.patternBlocks, dayCut=timePoint, showText=showText)
    else:
        return evaluator.create_svg(evaluator.patternBlocks, showText=showText)

if __name__ == '__main__':
    app.run(debug=True)