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
from duckdb_wrapper import DuckDBWrapper
from pattern import Pattern
from pattern_evaluator import PatternBlock, PatternEvaluator
from pattern_element import PatternElement
from pattern_routes import pattern_bp
from config import database_path

app = Flask(__name__)

app.register_blueprint(pattern_bp)

@app.route('/static/<path:filename>')
def static_files(filename: str) -> Response:
    return send_from_directory('static', filename)

@app.route('/')
@app.route('/desktop/')
def desktop() -> str:
    return render_template('desktop.html')

@app.route('/assets/')
def assets() -> str:
    return render_template('data_assets.html')

@app.route('/svg/generate', methods=['POST'])
def generate_svg() -> Response:
    data = request.json
    if not data or 'patternBlocks' not in data:
        return Response("Invalid data", status=400)
    
    svgType = request.args.get('type', 'full')
    latestWrittenElement = int(request.args.get('lw', 0))
    showText = request.args.get('text', 'false').lower() == 'true'
    showValue = request.args.get('val', 'false').lower() == 'true'

    patternBlocks = [PatternBlock(**block) for block in data['patternBlocks']]
    evaluator = PatternEvaluator(patternBlocks)

    if svgType == 'written':
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenElement=latestWrittenElement, condition="t", showText=showText, showValue=showValue)
    elif svgType == 'unwritten':
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenElement=latestWrittenElement, condition="b", showText=showText, showValue=showValue)
    elif svgType == 'lic':
        timePoint = evaluator.get_earliest_start_point_of_element(latestWrittenElement)
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenElement=latestWrittenElement, dayCut=timePoint, condition="tl", showText=showText, showValue=showValue)
    elif svgType == 'lrc':
        timePoint = evaluator.get_earliest_start_point_of_element(latestWrittenElement)
        svgContent = evaluator.create_svg(evaluator.patternBlocks, latestWrittenElement=latestWrittenElement, dayCut=timePoint, condition="tr", showText=showText, showValue=showValue)
    elif svgType == 'upr':
        timePoint = evaluator.get_earliest_start_point_of_element(latestWrittenElement)
        svgContent = evaluator.create_svg(evaluator.patternBlocks, dayCut=timePoint, showText=showText, condition="r", showValue=showValue)
    else:
        svgContent = evaluator.create_svg(evaluator.patternBlocks, colour='white', showText=showText, showValue=showValue)

    return Response(svgContent, mimetype='image/svg+xml')

@app.route('/data_assets')
def data_assets() -> Response:
    db_wrapper = DuckDBWrapper(database_path)
    db_wrapper.connect()
    data_assets = db_wrapper.execute_named_query_to_json("read_assets")
    print("XXXXXXXXX:" + data_assets)
    db_wrapper.close()
    return Response(data_assets, mimetype='application/json')

if __name__ == '__main__':
    app.run(debug=True)